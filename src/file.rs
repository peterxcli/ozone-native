use crate::api::{Client, WriteOptions};
use crate::block_entry_pool::BlockEntryPool;
use crate::client::OzoneClient;
use crate::error::{Error, Result};
use crate::om::{KeyReplication, OpenKeySession};
use crate::proto::hadoop::ozone;
use crate::status::FileStatus;
use crate::util::{key_replication, latest_key_locations, DEFAULT_BLOCK_SIZE};
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::Instrument;

pub struct FileReader {
    data: Bytes,
    position: usize,
}

impl FileReader {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data: Bytes::from(data),
            position: 0,
        }
    }

    pub fn file_length(&self) -> usize {
        self.data.len()
    }

    pub fn remaining(&self) -> usize {
        self.file_length().saturating_sub(self.position)
    }

    pub fn seek(&mut self, pos: usize) {
        if pos > self.file_length() {
            panic!("Cannot seek beyond the end of a file");
        }
        self.position = pos;
    }

    pub fn tell(&self) -> usize {
        self.position
    }

    pub async fn read(&mut self, len: usize) -> Result<Bytes> {
        let start = self.position;
        let end = usize::min(start + len, self.file_length());
        self.position = end;
        Ok(self.data.slice(start..end))
    }

    pub async fn read_buf(&mut self, buf: &mut [u8]) -> Result<usize> {
        let bytes = self.read(buf.len()).await?;
        let len = bytes.len();
        buf[..len].copy_from_slice(&bytes);
        Ok(len)
    }

    pub async fn read_range(&self, offset: usize, len: usize) -> Result<Bytes> {
        self.check_range(offset, len);
        Ok(self.data.slice(offset..offset + len))
    }

    pub async fn read_range_buf(&self, buf: &mut [u8], offset: usize) -> Result<()> {
        let bytes = self.read_range(offset, buf.len()).await?;
        buf.copy_from_slice(&bytes);
        Ok(())
    }

    pub fn read_range_stream(
        &self,
        offset: usize,
        len: usize,
    ) -> BoxStream<'static, Result<Bytes>> {
        self.check_range(offset, len);
        let bytes = self.data.slice(offset..offset + len);
        Box::pin(stream::once(async move { Ok(bytes) }))
    }

    fn check_range(&self, offset: usize, len: usize) {
        if offset
            .checked_add(len)
            .is_none_or(|end| end > self.file_length())
        {
            panic!("Cannot read past end of the file");
        }
    }
}

pub struct FileWriter {
    client: Arc<OzoneClient>,
    volume: String,
    bucket: String,
    key: String,
    replication: KeyReplication,
    open: Option<OpenKeySession>,
    pool: BlockEntryPool,
    bytes_written: u64,
    closed: bool,
}

impl FileWriter {
    pub(crate) async fn create(
        client: Arc<OzoneClient>,
        volume: String,
        bucket: String,
        key: String,
        options: WriteOptions,
    ) -> Result<Self> {
        let open = client
            .create_file_for_write(
                &volume,
                &bucket,
                &key,
                options.create_parent,
                options.overwrite,
                options.replication,
            )
            .await?;
        Ok(Self::new(
            client,
            volume,
            bucket,
            key,
            open,
            effective_block_size(&options),
        ))
    }

    fn new(
        client: Arc<OzoneClient>,
        volume: String,
        bucket: String,
        key: String,
        open: OpenKeySession,
        block_size: u64,
    ) -> Self {
        let (replication_type, factor, ec_replication) = key_replication(&open.key_info);
        let locations = latest_key_locations(&open.key_info);
        Self {
            client,
            volume,
            bucket,
            key,
            replication: KeyReplication {
                replication_type,
                factor,
                ec_replication,
            },
            open: Some(open),
            pool: BlockEntryPool::new(locations, block_size),
            bytes_written: 0,
            closed: false,
        }
    }

    pub async fn write(&mut self, buf: Bytes) -> Result<usize> {
        let len = buf.len();
        let file_offset = self.bytes_written;
        async {
            if self.closed {
                return Err(Error::InvalidState(
                    "cannot write to a closed FileWriter".to_string(),
                ));
            }

            let mut offset = 0usize;
            while offset < len {
                self.ensure_current_block().await?;
                if let Err(err) = self.ensure_lookahead().await {
                    tracing::warn!(
                        error = %err,
                        "failed to preallocate lookahead block; continuing with current block"
                    );
                }
                let write_len = self
                    .pool
                    .current_block()
                    .expect("current block")
                    .next_write_len(len - offset);

                if write_len == 0 {
                    self.close_current_block().await?;
                    self.pool.advance_past_full_current();
                    continue;
                }

                let block_file_offset = self.bytes_written;
                self.write_current_slice(&buf[offset..offset + write_len])
                    .instrument(tracing::info_span!(
                        "ozone.file.block_write",
                        file_offset = block_file_offset,
                        len = write_len
                    ))
                    .await?;
                self.bytes_written += write_len as u64;
                offset += write_len;

                if self.pool.current_is_full() {
                    self.close_current_block().await?;
                    self.pool.advance_past_full_current();
                }
            }
            Ok(len)
        }
        .instrument(tracing::info_span!("ozone.file.write", file_offset, len))
        .await
    }

    pub async fn close(&mut self) -> Result<()> {
        let bytes_written = self.bytes_written;
        async {
            if !self.closed {
                self.close_current_block().await?;
                let open_id = self
                    .open
                    .as_ref()
                    .ok_or_else(|| Error::InvalidState("file writer session missing".to_string()))?
                    .id;
                self.client
                    .commit_open_file(
                        &self.volume,
                        &self.bucket,
                        &self.key,
                        self.bytes_written,
                        open_id,
                        self.pool.committed_locations(),
                        &self.replication,
                    )
                    .await?;
                self.open = None;
                self.closed = true;
            }
            Ok(())
        }
        .instrument(tracing::info_span!("ozone.file.close", bytes_written))
        .await
    }

    async fn ensure_current_block(&mut self) -> Result<()> {
        if self.pool.needs_current_allocation() {
            let location = self
                .allocate_block()
                .instrument(tracing::info_span!(
                    "ozone.file.allocate_current_block",
                    file_offset = self.bytes_written
                ))
                .await?;
            self.pool.push_block(location);
        }
        Ok(())
    }

    async fn ensure_lookahead(&mut self) -> Result<()> {
        if self.pool.needs_lookahead() {
            let location = self
                .allocate_block()
                .instrument(tracing::info_span!(
                    "ozone.file.allocate_lookahead_block",
                    file_offset = self.bytes_written
                ))
                .await?;
            self.pool.push_block(location);
        }
        Ok(())
    }

    async fn allocate_block(&self) -> Result<ozone::KeyLocation> {
        let open = self
            .open
            .as_ref()
            .ok_or_else(|| Error::InvalidState("file writer session missing".to_string()))?;
        self.client
            .allocate_file_block_for_write(
                &self.volume,
                &self.bucket,
                &self.key,
                self.bytes_written,
                open.id,
                &self.replication,
                None,
            )
            .await
    }

    async fn write_current_slice(&mut self, data: &[u8]) -> Result<()> {
        self.ensure_current_writer().await?;
        let current = self
            .pool
            .current_block_mut()
            .ok_or_else(|| Error::InvalidState("current block missing".to_string()))?;
        current.write(data).await?;
        Ok(())
    }

    async fn ensure_current_writer(&mut self) -> Result<()> {
        let needs_writer = self
            .pool
            .current_block()
            .ok_or_else(|| Error::InvalidState("current block missing".to_string()))?
            .needs_writer();
        if needs_writer {
            let location = self
                .pool
                .current_location()
                .ok_or_else(|| Error::InvalidState("current block location missing".to_string()))?
                .clone();
            let writer = self.client.create_block_writer(&location).await?;
            self.pool
                .current_block_mut()
                .ok_or_else(|| Error::InvalidState("current block missing".to_string()))?
                .attach_writer(writer)?;
        }
        Ok(())
    }

    async fn close_current_block(&mut self) -> Result<()> {
        let Some(current) = self.pool.current_block_mut() else {
            return Ok(());
        };
        current
            .close()
            .instrument(tracing::info_span!(
                "ozone.file.close_current_block",
                file_offset = self.bytes_written
            ))
            .await
    }
}

fn effective_block_size(options: &WriteOptions) -> u64 {
    options
        .block_size
        .filter(|block_size| *block_size > 0)
        .unwrap_or(DEFAULT_BLOCK_SIZE)
}

#[derive(Clone)]
pub struct ListStatusIterator {
    client: Client,
    volume: String,
    bucket: String,
    key: String,
    recursive: bool,
    state: Arc<Mutex<ListStatusState>>,
}

#[derive(Default)]
struct ListStatusState {
    finished: bool,
    start_key: String,
    statuses: VecDeque<FileStatus>,
}

impl ListStatusIterator {
    pub(crate) fn new(
        client: Client,
        volume: &str,
        bucket: &str,
        key: &str,
        recursive: bool,
    ) -> Self {
        Self {
            client,
            volume: volume.to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            recursive,
            state: Arc::new(Mutex::new(ListStatusState::default())),
        }
    }

    pub async fn next(&self) -> Option<Result<FileStatus>> {
        loop {
            let mut state = self.state.lock().await;
            if let Some(status) = state.statuses.pop_front() {
                return Some(Ok(status));
            }
            if state.finished {
                return None;
            }

            let start_key = state.start_key.clone();
            match self
                .client
                .list_status_page(
                    &self.volume,
                    &self.bucket,
                    &self.key,
                    self.recursive,
                    &start_key,
                )
                .await
            {
                Ok(page) => {
                    state.start_key = page.next_start_key.unwrap_or_default();
                    state.finished = !page.has_more;
                    state.statuses = page.statuses.into();
                    if state.finished && state.statuses.is_empty() {
                        return None;
                    }
                }
                Err(err) => {
                    state.finished = true;
                    return Some(Err(err));
                }
            }
        }
    }

    pub fn into_stream(self) -> BoxStream<'static, Result<FileStatus>> {
        Box::pin(stream::unfold(self, |iterator| async move {
            iterator.next().await.map(|item| (item, iterator))
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::FileReader;
    use bytes::Bytes;
    use futures::StreamExt;

    #[tokio::test]
    async fn reader_tracks_cursor_for_sequential_reads() {
        let mut reader = FileReader::new(b"abcdef".to_vec());

        assert_eq!(reader.file_length(), 6);
        assert_eq!(reader.remaining(), 6);
        assert_eq!(reader.tell(), 0);
        assert_eq!(reader.read(2).await.unwrap(), Bytes::from_static(b"ab"));
        assert_eq!(reader.tell(), 2);
        assert_eq!(reader.remaining(), 4);
        assert_eq!(reader.read(10).await.unwrap(), Bytes::from_static(b"cdef"));
        assert_eq!(reader.read(1).await.unwrap(), Bytes::new());
    }

    #[tokio::test]
    async fn reader_range_reads_do_not_move_cursor() {
        let mut reader = FileReader::new(b"abcdef".to_vec());
        reader.seek(3);

        assert_eq!(
            reader.read_range(1, 3).await.unwrap(),
            Bytes::from_static(b"bcd")
        );
        assert_eq!(reader.tell(), 3);

        let mut buf = [0; 2];
        reader.read_range_buf(&mut buf, 4).await.unwrap();
        assert_eq!(&buf, b"ef");
        assert_eq!(reader.tell(), 3);
    }

    #[tokio::test]
    async fn reader_streams_ranges() {
        let reader = FileReader::new(b"abcdef".to_vec());
        let chunks = reader
            .read_range_stream(2, 3)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(chunks, vec![Bytes::from_static(b"cde")]);
    }

    #[test]
    #[should_panic(expected = "Cannot seek beyond the end of a file")]
    fn seek_past_end_panics() {
        FileReader::new(b"abc".to_vec()).seek(4);
    }

    #[tokio::test]
    #[should_panic(expected = "Cannot read past end of the file")]
    async fn range_past_end_panics() {
        let reader = FileReader::new(b"abc".to_vec());
        let _ = reader.read_range(2, 2).await;
    }
}
