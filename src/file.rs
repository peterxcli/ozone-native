use crate::api::{Client, WriteOptions};
use crate::block_writer::BlockWriter;
use crate::client::OzoneClient;
use crate::error::{Error, Result};
use crate::om::{BlockAllocateExcludeList, KeyReplication, OpenKeySession};
use crate::proto::hadoop::ozone;
use crate::status::FileStatus;
use crate::util::{key_replication, latest_key_locations};
use bytes::{Bytes, BytesMut};
use futures::stream::{self, BoxStream};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

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
    pending: VecDeque<ozone::KeyLocation>,
    active: Option<ActiveBlock>,
    committed: Vec<ozone::KeyLocation>,
    bytes_written: u64,
    closed: bool,
}

#[derive(Clone, Copy, Debug)]
struct ActiveBlockProgress {
    capacity: Option<u64>,
    written: u64,
}

impl ActiveBlockProgress {
    fn bounded(capacity: u64) -> Self {
        Self {
            capacity: Some(capacity),
            written: 0,
        }
    }

    fn from_location_len(len: u64) -> Self {
        if len == 0 {
            Self {
                capacity: None,
                written: 0,
            }
        } else {
            Self::bounded(len)
        }
    }

    fn next_write_len(&self, requested: usize) -> usize {
        self.remaining()
            .map(|remaining| requested.min(remaining as usize))
            .unwrap_or(requested)
    }

    fn observe_write(&mut self, len: u64) {
        self.written += len;
    }

    fn is_full(&self) -> bool {
        self.remaining() == Some(0)
    }

    fn remaining(&self) -> Option<u64> {
        self.capacity
            .map(|capacity| capacity.saturating_sub(self.written))
    }
}

struct ActiveBlock {
    location: ozone::KeyLocation,
    writer: Option<BlockWriter>,
    progress: ActiveBlockProgress,
    data: BytesMut,
}

impl ActiveBlock {
    fn new(location: ozone::KeyLocation, writer: BlockWriter) -> Self {
        Self {
            progress: ActiveBlockProgress::from_location_len(location.length),
            location,
            writer: Some(writer),
            data: BytesMut::new(),
        }
    }

    fn next_write_len(&self, requested: usize) -> usize {
        self.progress.next_write_len(requested)
    }

    fn observe_write(&mut self, len: usize) {
        self.progress.observe_write(len as u64);
    }

    fn is_full(&self) -> bool {
        self.progress.is_full()
    }
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
        Ok(Self::new(client, volume, bucket, key, open))
    }

    fn new(
        client: Arc<OzoneClient>,
        volume: String,
        bucket: String,
        key: String,
        open: OpenKeySession,
    ) -> Self {
        let (replication_type, factor, ec_replication) = key_replication(&open.key_info);
        let pending = latest_key_locations(&open.key_info).into();
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
            pending,
            active: None,
            committed: Vec::new(),
            bytes_written: 0,
            closed: false,
        }
    }

    pub async fn write(&mut self, buf: Bytes) -> Result<usize> {
        if self.closed {
            return Err(Error::InvalidState(
                "cannot write to a closed FileWriter".to_string(),
            ));
        }

        let len = buf.len();
        let mut offset = 0usize;
        while offset < len {
            self.ensure_active_block().await?;
            let write_len = self
                .active
                .as_ref()
                .expect("active block")
                .next_write_len(len - offset);

            if write_len == 0 {
                self.close_active_block().await?;
                continue;
            }

            self.write_active_slice(&buf[offset..offset + write_len])
                .await?;
            self.bytes_written += write_len as u64;
            offset += write_len;

            if self.active.as_ref().is_some_and(ActiveBlock::is_full) {
                self.close_active_block().await?;
            }
        }
        Ok(len)
    }

    pub async fn close(&mut self) -> Result<()> {
        if !self.closed {
            self.close_active_block().await?;
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
                    self.committed.clone(),
                    &self.replication,
                )
                .await?;
            self.open = None;
            self.closed = true;
        }
        Ok(())
    }

    async fn ensure_active_block(&mut self) -> Result<()> {
        if self.active.is_some() {
            return Ok(());
        }

        let location = match self.pending.pop_front() {
            Some(location) => location,
            None => self.allocate_block(None).await?,
        };
        let writer = self.client.create_block_writer(&location).await?;
        self.active = Some(ActiveBlock::new(location, writer));
        Ok(())
    }

    async fn allocate_block(
        &self,
        exclude: Option<&BlockAllocateExcludeList>,
    ) -> Result<ozone::KeyLocation> {
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
                exclude,
            )
            .await
    }

    async fn write_active_slice(&mut self, data: &[u8]) -> Result<()> {
        {
            let active = self
                .active
                .as_mut()
                .ok_or_else(|| Error::InvalidState("active file block missing".to_string()))?;
            active.data.extend_from_slice(data);
        }

        let write_result = {
            let active = self
                .active
                .as_mut()
                .ok_or_else(|| Error::InvalidState("active file block missing".to_string()))?;
            let writer = active
                .writer
                .as_mut()
                .ok_or_else(|| Error::InvalidState("active block writer missing".to_string()))?;
            writer.write(data).await
        };

        match write_result {
            Ok(()) => {
                if let Some(active) = self.active.as_mut() {
                    active.observe_write(data.len());
                }
                Ok(())
            }
            Err(err) => {
                let mut exclude = BlockAllocateExcludeList::default();
                if let Some(active) = self.active.as_ref() {
                    add_location_pipeline_to_exclude(&active.location, &mut exclude);
                }
                self.replace_active_block_with_replay(&mut exclude, Some(err))
                    .await
            }
        }
    }

    async fn close_active_block(&mut self) -> Result<()> {
        let mut exclude = BlockAllocateExcludeList::default();
        loop {
            let Some(mut active) = self.active.take() else {
                return Ok(());
            };
            let writer = active
                .writer
                .take()
                .ok_or_else(|| Error::InvalidState("active block writer missing".to_string()))?;
            match writer.close().await {
                Ok(location) => {
                    self.committed.push(location);
                    return Ok(());
                }
                Err(err) => {
                    add_location_pipeline_to_exclude(&active.location, &mut exclude);
                    self.active = Some(active);
                    self.replace_active_block_with_replay(&mut exclude, Some(err))
                        .await?;
                }
            }
        }
    }

    async fn replace_active_block_with_replay(
        &mut self,
        exclude: &mut BlockAllocateExcludeList,
        initial_error: Option<Error>,
    ) -> Result<()> {
        let data = self
            .active
            .as_ref()
            .ok_or_else(|| Error::InvalidState("active file block missing".to_string()))?
            .data
            .clone();
        let mut last_error = initial_error;

        for _ in 0..=self.client.max_write_retries() {
            let location = self.allocate_block(Some(exclude)).await?;
            match self.replay_active_block(location.clone(), &data).await {
                Ok(active) => {
                    self.active = Some(active);
                    return Ok(());
                }
                Err(err) => {
                    add_location_pipeline_to_exclude(&location, exclude);
                    last_error = Some(err);
                }
            }
        }

        Err(last_error
            .unwrap_or_else(|| Error::Ratis("exhausted block replacement attempts".to_string())))
    }

    async fn replay_active_block(
        &self,
        location: ozone::KeyLocation,
        data: &[u8],
    ) -> Result<ActiveBlock> {
        let mut writer = self.client.create_block_writer(&location).await?;
        if !data.is_empty() {
            writer.write(data).await?;
        }
        let mut active = ActiveBlock::new(location, writer);
        active.data.extend_from_slice(data);
        active.observe_write(data.len());
        Ok(active)
    }
}

fn add_location_pipeline_to_exclude(
    location: &ozone::KeyLocation,
    exclude: &mut BlockAllocateExcludeList,
) {
    if let Some(pipeline) = location.pipeline.as_ref() {
        exclude.pipeline_ids.push(pipeline.id.clone());
    }
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
    use super::{ActiveBlockProgress, FileReader};
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

    #[test]
    fn active_block_progress_caps_writes_at_remaining_capacity() {
        let mut progress = ActiveBlockProgress::bounded(10);

        assert_eq!(progress.next_write_len(6), 6);
        progress.observe_write(6);
        assert_eq!(progress.next_write_len(10), 4);
        progress.observe_write(4);

        assert!(progress.is_full());
        assert_eq!(progress.next_write_len(1), 0);
    }

    #[test]
    fn active_block_progress_treats_zero_length_locations_as_unbounded() {
        let mut progress = ActiveBlockProgress::from_location_len(0);

        assert_eq!(progress.next_write_len(16), 16);
        progress.observe_write(16);

        assert!(!progress.is_full());
        assert_eq!(progress.next_write_len(8), 8);
    }
}
