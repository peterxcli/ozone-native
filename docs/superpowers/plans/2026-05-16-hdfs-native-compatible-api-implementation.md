# HDFS-Native-Compatible API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `hdfs-native`-like public API on top of the existing Ozone client while preserving explicit `volume`, `bucket`, and `key` arguments.

**Architecture:** Add a thin compatibility layer that wraps `OzoneClient`, plus focused modules for file cursors, status conversion, and ACL conversion. Implement Ozone-backed methods where the OM protocol exposes matching operations and return explicit `Error::Unsupported` for the remaining compatibility methods.

**Tech Stack:** Rust 2021, Tokio, tonic/prost Ozone protos, bytes, futures, `cargo test`

---

## File Structure

- `src/api.rs`: `ClientBuilder`, `Client`, `IORuntime`, `WriteOptions`, public compatibility methods, and unsupported method errors.
- `src/file.rs`: in-memory `FileReader` and buffered `FileWriter`.
- `src/status.rs`: `FileStatus`, `ContentSummary`, and `OzoneFileStatusProto` conversion.
- `src/acl.rs`: `AclEntry`, `AclStatus`, ACL enums, and Ozone ACL conversion.
- `src/om.rs`: OM operations for create file, lookup file, get/list file status, create directory, set times, and ACL requests.
- `src/client.rs`: file-oriented helpers on `OzoneClient` and shared open-key write helper.
- `src/error.rs`: `InvalidArgument` error variant.
- `src/lib.rs`: module declarations and public re-exports.
- `tests/hdfs_api.rs`: API-surface compile and builder tests.

### Task 1: Public API Shell and Config Builder

**Files:**
- Create: `tests/hdfs_api.rs`
- Create: `src/api.rs`
- Modify: `src/error.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write the failing test**

```rust
use bytes::Bytes;
use ozone_rust::{
    AclEntry, AclEntryScope, AclEntryType, Client, ClientBuilder, FsAction, Result, WriteOptions,
};

#[test]
fn write_options_match_hdfs_native_builder_shape() {
    let options = WriteOptions::default()
        .block_size(128)
        .replication(3)
        .permission(0o755)
        .overwrite(true)
        .create_parent(false);

    assert_eq!(options.block_size, Some(128));
    assert_eq!(options.replication, Some(3));
    assert_eq!(options.permission, 0o755);
    assert!(options.overwrite);
    assert!(!options.create_parent);
}

#[test]
fn builder_rejects_unknown_config_keys() {
    let result = ClientBuilder::new()
        .with_url("http://127.0.0.1:9874")
        .with_config(vec![("ozone.unknown", "true")])
        .build_config_for_tests();

    assert!(result
        .expect_err("unknown config")
        .to_string()
        .contains("unknown client config key"));
}

#[allow(dead_code)]
async fn hdfs_like_methods_are_callable(client: Client) -> Result<()> {
    let options = WriteOptions::default().overwrite(true).create_parent(true);
    let mut writer = client.create("vol", "bucket", "key", options).await?;
    writer.write(Bytes::from_static(b"hello")).await?;
    writer.close().await?;

    let mut reader = client.read("vol", "bucket", "key").await?;
    let _ = reader.read(5).await?;
    let mut buf = [0; 5];
    let _ = reader.read_buf(&mut buf).await?;
    let _ = reader.read_range(0, 1).await?;
    reader.read_range_buf(&mut buf[..1], 0).await?;
    let _ = reader.read_range_stream(0, 1);

    let _ = client.get_file_info("vol", "bucket", "key").await?;
    let _ = client.list_status("vol", "bucket", "", false).await?;
    let _ = client.list_status_iter("vol", "bucket", "", true).into_stream();
    client.mkdirs("vol", "bucket", "dir", 0o755, true).await?;
    let _ = client.delete("vol", "bucket", "key", false).await?;
    client.set_times("vol", "bucket", "key", 1, 2).await?;

    let acl = AclEntry::new(
        AclEntryType::User,
        AclEntryScope::Access,
        FsAction::ReadWrite,
        Some("alice".to_string()),
    );
    client.modify_acl_entries("vol", "bucket", "key", vec![acl.clone()]).await?;
    client.remove_acl_entries("vol", "bucket", "key", vec![acl.clone()]).await?;
    client.set_acl("vol", "bucket", "key", vec![acl]).await?;
    client.remove_default_acl("vol", "bucket", "key").await?;
    client.remove_acl("vol", "bucket", "key").await?;
    let _ = client.get_acl_status("vol", "bucket", "key").await?;

    let _ = client.append("vol", "bucket", "key").await;
    let _ = client.rename("vol", "bucket", "a", "vol", "bucket", "b", false).await;
    let _ = client.set_owner("vol", "bucket", "key", Some("u"), Some("g")).await;
    let _ = client.set_permission("vol", "bucket", "key", 0o644).await;
    let _ = client.set_replication("vol", "bucket", "key", 3).await;
    let _ = client.get_content_summary("vol", "bucket", "key").await;
    let _ = client.glob_status("vol", "bucket", "*.txt").await;

    Ok(())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test hdfs_api`

Expected: FAIL with unresolved imports for `Client`, `ClientBuilder`, `WriteOptions`, and ACL types.

- [ ] **Step 3: Write minimal implementation**

Add `Error::InvalidArgument(String)`:

```rust
#[error("invalid argument: {0}")]
InvalidArgument(String),
```

Add `src/api.rs` with:

```rust
use crate::client::{ClientConfig, OzoneClient};
use crate::error::{Error, Result};
use crate::file::{FileReader, FileWriter, ListStatusIterator};
use crate::{AclEntry, AclStatus, ContentSummary, FileStatus};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::{Handle, Runtime};

#[derive(Debug)]
pub enum IORuntime {
    Runtime(Runtime),
    Handle(Handle),
}

impl From<Runtime> for IORuntime {
    fn from(value: Runtime) -> Self {
        Self::Runtime(value)
    }
}

impl From<Handle> for IORuntime {
    fn from(value: Handle) -> Self {
        Self::Handle(value)
    }
}

#[derive(Clone, Debug)]
pub struct WriteOptions {
    pub block_size: Option<u64>,
    pub replication: Option<u32>,
    pub permission: u32,
    pub overwrite: bool,
    pub create_parent: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            block_size: None,
            replication: None,
            permission: 0o644,
            overwrite: false,
            create_parent: true,
        }
    }
}

impl AsRef<WriteOptions> for WriteOptions {
    fn as_ref(&self) -> &WriteOptions {
        self
    }
}

impl WriteOptions {
    pub fn block_size(mut self, block_size: u64) -> Self {
        self.block_size = Some(block_size);
        self
    }

    pub fn replication(mut self, replication: u32) -> Self {
        self.replication = Some(replication);
        self
    }

    pub fn permission(mut self, permission: u32) -> Self {
        self.permission = permission;
        self
    }

    pub fn overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    pub fn create_parent(mut self, create_parent: bool) -> Self {
        self.create_parent = create_parent;
        self
    }
}

#[derive(Default)]
pub struct ClientBuilder {
    url: Option<String>,
    config: Option<HashMap<String, String>>,
    config_dir: Option<String>,
    runtime: Option<IORuntime>,
}

impl ClientBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    pub fn with_config(
        mut self,
        config: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.config = Some(
            config
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        );
        self
    }

    pub fn with_config_dir(mut self, config_dir: impl Into<String>) -> Self {
        self.config_dir = Some(config_dir.into());
        self
    }

    pub fn with_io_runtime(mut self, runtime: impl Into<IORuntime>) -> Self {
        self.runtime = Some(runtime.into());
        self
    }

    pub async fn build(self) -> Result<Client> {
        if self.config_dir.is_some() {
            return Err(Error::Unsupported(
                "ClientBuilder::with_config_dir is not supported; pass the Ozone OM URL with with_url".to_string(),
            ));
        }
        let endpoint = self.url.clone().ok_or_else(|| {
            Error::InvalidArgument("ClientBuilder requires with_url for the Ozone OM endpoint".to_string())
        })?;
        let config = self.build_config_for_tests()?;
        let ozone = OzoneClient::connect_with_config(&endpoint, config).await?;
        Ok(Client::from_ozone(ozone))
    }

    pub fn build_config_for_tests(&self) -> Result<ClientConfig> {
        let mut config = ClientConfig::default();
        if let Some(values) = &self.config {
            for (key, value) in values {
                apply_config(&mut config, key, value)?;
            }
        }
        Ok(config)
    }
}

#[derive(Clone)]
pub struct Client {
    inner: Arc<OzoneClient>,
}

impl Client {
    pub fn from_ozone(inner: OzoneClient) -> Self {
        Self { inner: Arc::new(inner) }
    }

    pub async fn read(&self, volume: &str, bucket: &str, key: &str) -> Result<FileReader> {
        Ok(FileReader::new(self.inner.get_file_bytes(volume, bucket, key).await?))
    }

    pub async fn create(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        write_options: impl AsRef<WriteOptions>,
    ) -> Result<FileWriter> {
        Ok(FileWriter::new(
            Arc::clone(&self.inner),
            volume.to_string(),
            bucket.to_string(),
            key.to_string(),
            write_options.as_ref().clone(),
        ))
    }

    pub async fn get_file_info(&self, volume: &str, bucket: &str, key: &str) -> Result<FileStatus> {
        self.inner.get_file_status(volume, bucket, key).await
    }

    pub async fn list_status(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        recursive: bool,
    ) -> Result<Vec<FileStatus>> {
        self.inner.list_status(volume, bucket, key, recursive).await
    }

    pub fn list_status_iter(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        recursive: bool,
    ) -> ListStatusIterator {
        ListStatusIterator::new(self.clone(), volume, bucket, key, recursive)
    }

    pub async fn mkdirs(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        _permission: u32,
        create_parent: bool,
    ) -> Result<()> {
        self.inner.create_directory(volume, bucket, key, create_parent).await
    }

    pub async fn delete(&self, volume: &str, bucket: &str, key: &str, _recursive: bool) -> Result<bool> {
        self.inner.delete_key(volume, bucket, key).await?;
        Ok(true)
    }

    pub async fn set_times(&self, volume: &str, bucket: &str, key: &str, mtime: u64, atime: u64) -> Result<()> {
        self.inner.set_times(volume, bucket, key, mtime, atime).await
    }

    pub async fn modify_acl_entries(&self, volume: &str, bucket: &str, key: &str, acl_spec: Vec<AclEntry>) -> Result<()> {
        self.inner.add_acls(volume, bucket, key, acl_spec).await
    }

    pub async fn remove_acl_entries(&self, volume: &str, bucket: &str, key: &str, acl_spec: Vec<AclEntry>) -> Result<()> {
        self.inner.remove_acls(volume, bucket, key, acl_spec).await
    }

    pub async fn remove_default_acl(&self, volume: &str, bucket: &str, key: &str) -> Result<()> {
        let current = self.get_acl_status(volume, bucket, key).await?;
        let retained = current
            .entries
            .into_iter()
            .filter(|entry| !entry.is_default())
            .collect();
        self.set_acl(volume, bucket, key, retained).await
    }

    pub async fn remove_acl(&self, volume: &str, bucket: &str, key: &str) -> Result<()> {
        self.set_acl(volume, bucket, key, Vec::new()).await
    }

    pub async fn set_acl(&self, volume: &str, bucket: &str, key: &str, acl_spec: Vec<AclEntry>) -> Result<()> {
        self.inner.set_acls(volume, bucket, key, acl_spec).await
    }

    pub async fn get_acl_status(&self, volume: &str, bucket: &str, key: &str) -> Result<AclStatus> {
        self.inner.get_acl_status(volume, bucket, key).await
    }

    pub async fn append(&self, _volume: &str, _bucket: &str, _key: &str) -> Result<FileWriter> {
        unsupported("Client::append", "Ozone append is not implemented by this compatibility layer")
    }

    pub async fn rename(
        &self,
        _src_volume: &str,
        _src_bucket: &str,
        _src_key: &str,
        _dst_volume: &str,
        _dst_bucket: &str,
        _dst_key: &str,
        _overwrite: bool,
    ) -> Result<()> {
        unsupported("Client::rename", "Ozone rename is not implemented by this compatibility layer")
    }

    pub async fn set_owner(&self, _volume: &str, _bucket: &str, _key: &str, _owner: Option<&str>, _group: Option<&str>) -> Result<()> {
        unsupported("Client::set_owner", "Ozone does not expose HDFS owner/group mutation here")
    }

    pub async fn set_permission(&self, _volume: &str, _bucket: &str, _key: &str, _permission: u32) -> Result<()> {
        unsupported("Client::set_permission", "Ozone does not expose HDFS POSIX permission mutation here")
    }

    pub async fn set_replication(&self, _volume: &str, _bucket: &str, _key: &str, _replication: u32) -> Result<bool> {
        unsupported("Client::set_replication", "Ozone replication mutation is not implemented by this compatibility layer")
    }

    pub async fn get_content_summary(&self, _volume: &str, _bucket: &str, _key: &str) -> Result<ContentSummary> {
        unsupported("Client::get_content_summary", "Ozone content summary is not implemented by this compatibility layer")
    }

    pub async fn glob_status(&self, _volume: &str, _bucket: &str, _pattern: &str) -> Result<Vec<FileStatus>> {
        unsupported("Client::glob_status", "Hadoop-style globbing is not implemented by this compatibility layer")
    }
}

fn unsupported<T>(method: &str, reason: &str) -> Result<T> {
    Err(Error::Unsupported(format!("{method}: {reason}")))
}

fn apply_config(config: &mut ClientConfig, key: &str, value: &str) -> Result<()> {
    match key {
        "ozone.chunk.size" => config.chunk_size = parse_usize(key, value)?,
        "ozone.stream.flush.size" => config.stream_flush_size = parse_usize(key, value)?,
        "ozone.stream.window.size" => config.stream_window_size = parse_usize(key, value)?,
        "ozone.read.response.size" => config.read_response_size = parse_u32(key, value)?,
        "ozone.watch.for.commit" => config.watch_for_commit = parse_bool(key, value)?,
        "ozone.max.write.retries" => config.max_write_retries = parse_usize(key, value)?,
        "ozone.enable.put.block.piggybacking" => config.enable_put_block_piggybacking = parse_bool(key, value)?,
        "ozone.enable.incremental.chunk.list" => config.enable_incremental_chunk_list = parse_bool(key, value)?,
        "ozone.host.override" => config.host_override = Some(value.to_string()),
        _ => return Err(Error::InvalidArgument(format!("unknown client config key `{key}`"))),
    }
    Ok(())
}

fn parse_usize(key: &str, value: &str) -> Result<usize> {
    value.parse().map_err(|_| Error::InvalidArgument(format!("invalid usize for `{key}`: {value}")))
}

fn parse_u32(key: &str, value: &str) -> Result<u32> {
    value.parse().map_err(|_| Error::InvalidArgument(format!("invalid u32 for `{key}`: {value}")))
}

fn parse_bool(key: &str, value: &str) -> Result<bool> {
    value.parse().map_err(|_| Error::InvalidArgument(format!("invalid bool for `{key}`: {value}")))
}
```

Update `src/lib.rs`:

```rust
mod acl;
mod api;
mod file;
mod status;

pub use acl::{AclEntry, AclEntryScope, AclEntryType, AclStatus, FsAction};
pub use api::{Client, ClientBuilder, IORuntime, WriteOptions};
pub use file::{FileReader, FileWriter, ListStatusIterator};
pub use status::{ContentSummary, FileStatus};
```

- [ ] **Step 4: Run test to verify the public API shell resolves**

Run: `cargo test --test hdfs_api`

Expected: FAIL only on unresolved `crate::file`, `crate::status`, and `crate::acl` modules. The unresolved public API imports from step 2 are resolved.

- [ ] **Step 5: Commit after green**

Run after Task 4 is green:

```bash
git add tests/hdfs_api.rs src/api.rs src/error.rs src/lib.rs
git commit -m "feat: add hdfs-compatible api shell"
```

### Task 2: File Reader, Writer, and List Iterator

**Files:**
- Create: `src/file.rs`
- Test: `src/file.rs`

- [ ] **Step 1: Write the failing tests**

```rust
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

        assert_eq!(reader.read_range(1, 3).await.unwrap(), Bytes::from_static(b"bcd"));
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test file::tests --lib`

Expected: FAIL because `FileReader` methods do not exist yet.

- [ ] **Step 3: Write minimal implementation**

Implement `FileReader`, `FileWriter`, and `ListStatusIterator`:

```rust
use crate::api::{Client, WriteOptions};
use crate::client::OzoneClient;
use crate::error::Result;
use crate::status::FileStatus;
use bytes::{Bytes, BytesMut};
use futures::stream::{self, BoxStream};
use std::sync::Arc;

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

    pub fn read_range_stream(&self, offset: usize, len: usize) -> BoxStream<'static, Result<Bytes>> {
        self.check_range(offset, len);
        Box::pin(stream::once(async move {
            Ok(Bytes::copy_from_slice(&self.data.slice(offset..offset + len)))
        }))
    }

    fn check_range(&self, offset: usize, len: usize) {
        if offset.checked_add(len).is_none_or(|end| end > self.file_length()) {
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
}

impl ListStatusIterator {
    pub(crate) fn new(client: Client, volume: &str, bucket: &str, key: &str, recursive: bool) -> Self {
        Self {
            client,
            volume: volume.to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            recursive,
        }
    }

    pub async fn next(&self) -> Option<Result<FileStatus>> {
        let mut statuses = match self
            .client
            .list_status(&self.volume, &self.bucket, &self.key, self.recursive)
            .await
        {
            Ok(statuses) => statuses,
            Err(err) => return Some(Err(err)),
        };
        statuses.into_iter().next().map(Ok)
    }

    pub fn into_stream(self) -> BoxStream<'static, Result<FileStatus>> {
        Box::pin(stream::once(async move {
            self.client
                .list_status(&self.volume, &self.bucket, &self.key, self.recursive)
                .await
        })
        .flat_map(|result| stream::iter(match result {
            Ok(statuses) => statuses.into_iter().map(Ok).collect::<Vec<_>>(),
            Err(err) => vec![Err(err)],
        })))
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test file::tests --lib`

Expected: PASS.

- [ ] **Step 5: Commit after green**

```bash
git add src/file.rs
git commit -m "feat: add hdfs-compatible file cursors"
```

### Task 3: Status and ACL Conversion Types

**Files:**
- Create: `src/status.rs`
- Create: `src/acl.rs`

- [ ] **Step 1: Write the failing tests**

Add tests inside `src/status.rs` and `src/acl.rs` that build Ozone proto values and verify conversion to compatibility types.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test status::tests acl::tests --lib`

Expected: FAIL because conversion implementations are missing.

- [ ] **Step 3: Write minimal implementation**

Implement:

- `FileStatus::from_ozone(status: ozone::OzoneFileStatusProto) -> Result<FileStatus>`
- `ContentSummary`
- `AclEntryType`, `AclEntryScope`, `FsAction`, `AclEntry`, `AclStatus`
- `AclEntry::to_ozone() -> ozone::OzoneAclInfo`
- `AclEntry::from_ozone(ozone::OzoneAclInfo) -> Result<AclEntry>`
- `key_obj(volume, bucket, key) -> ozone::OzoneObj`

Use Ozone ACL bit positions: `READ=0`, `WRITE=1`, `CREATE=2`, `LIST=3`, `DELETE=4`, `READ_ACL=5`, `WRITE_ACL=6`, `ALL=7`, `NONE=8`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test status::tests acl::tests --lib`

Expected: PASS.

- [ ] **Step 5: Commit after green**

```bash
git add src/status.rs src/acl.rs
git commit -m "feat: add hdfs-compatible status and acl types"
```

### Task 4: OzoneClient and OM Filesystem Methods

**Files:**
- Modify: `src/om.rs`
- Modify: `src/client.rs`

- [ ] **Step 1: Write the failing tests**

Add unit tests for pure request helpers:

- `key_args_for_file_request_sets_recursive_flag`
- `key_acl_object_uses_ozone_key_path`
- `open_key_write_helper_reuses_create_file_session`

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test om::tests client::tests --lib`

Expected: FAIL because helper functions and file APIs are missing.

- [ ] **Step 3: Write minimal implementation**

In `src/om.rs`, add:

- `create_file(volume, bucket, key, data_size, recursive, overwrite, replication) -> Result<OpenKeySession>`
- `lookup_file(volume, bucket, key) -> Result<ozone::KeyInfo>`
- `get_file_status(volume, bucket, key) -> Result<ozone::OzoneFileStatusProto>`
- `list_status(volume, bucket, key, recursive, start_key, num_entries) -> Result<Vec<ozone::OzoneFileStatusProto>>`
- `create_directory(volume, bucket, key, recursive) -> Result<()>`
- `set_times(volume, bucket, key, mtime, atime) -> Result<()>`
- `get_acl`, `add_acl`, `remove_acl`, `set_acl`

In `src/client.rs`, add:

- private `write_open_key_bytes`
- `put_file_bytes`
- `get_file_status`
- `list_status`
- `create_directory`
- `set_times`
- `get_acl_status`
- `add_acls`
- `remove_acls`
- `set_acls`

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test om::tests client::tests --lib`

Expected: PASS.

- [ ] **Step 5: Commit after green**

```bash
git add src/om.rs src/client.rs
git commit -m "feat: add ozone filesystem api mappings"
```

### Task 5: Full API Verification

**Files:**
- Modify: `tests/ozone_cluster.rs`

- [ ] **Step 1: Add compatibility integration coverage**

Add ignored cluster tests for:

- `ClientBuilder`
- `create`/`write`/`close`
- `read`
- `get_file_info`
- `list_status`
- `delete`

- [ ] **Step 2: Run focused tests**

Run:

```bash
cargo test --test hdfs_api
cargo test file::tests status::tests acl::tests --lib
cargo test
```

Expected: all non-ignored tests pass; cluster integration tests remain ignored unless explicitly requested.

- [ ] **Step 3: Run formatting and final verification**

Run:

```bash
cargo fmt --check
cargo test
```

Expected: formatting check passes and all non-ignored tests pass.

- [ ] **Step 4: Commit final adjustments**

```bash
git add tests/ozone_cluster.rs README.md
git commit -m "test: cover hdfs-compatible api"
```
