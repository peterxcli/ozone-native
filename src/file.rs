use crate::api::{Client, WriteOptions};
use crate::client::OzoneClient;
use crate::error::Result;
use crate::status::FileStatus;
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
    options: WriteOptions,
    data: BytesMut,
    closed: bool,
}

impl FileWriter {
    pub(crate) fn new(
        client: Arc<OzoneClient>,
        volume: String,
        bucket: String,
        key: String,
        options: WriteOptions,
    ) -> Self {
        Self {
            client,
            volume,
            bucket,
            key,
            options,
            data: BytesMut::new(),
            closed: false,
        }
    }

    pub async fn write(&mut self, buf: Bytes) -> Result<usize> {
        let len = buf.len();
        self.data.extend_from_slice(&buf);
        Ok(len)
    }

    pub async fn close(&mut self) -> Result<()> {
        if !self.closed {
            self.client
                .put_file_bytes(
                    &self.volume,
                    &self.bucket,
                    &self.key,
                    &self.data,
                    self.options.create_parent,
                    self.options.overwrite,
                    self.options.replication,
                )
                .await?;
            self.closed = true;
        }
        Ok(())
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
