use crate::client::{ClientConfig, ListStatusPage, OzoneClient, LIST_STATUS_PAGE_SIZE};
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
    /// Client-side target block size for streaming writes when Ozone returns
    /// zero-length block allocations.
    pub block_size: Option<u64>,
    /// Optional RATIS replication factor for file creation. `None` uses the
    /// bucket/server default replication.
    pub replication: Option<u32>,
    /// POSIX permission accepted for API parity. Ozone file create does not
    /// expose this field in the OM request used by this client.
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
                "ClientBuilder::with_config_dir is not supported; pass the Ozone OM URL with with_url"
                    .to_string(),
            ));
        }
        let endpoint = self.resolve_endpoint()?;
        let config = self.build_config()?;
        let ozone = OzoneClient::connect_with_config(&endpoint, config).await?;
        Ok(Client::from_ozone(ozone))
    }

    pub fn build_config(&self) -> Result<ClientConfig> {
        let mut config = ClientConfig::default();
        if let Some(values) = &self.config {
            for (key, value) in values {
                apply_config(&mut config, key, value)?;
            }
        }
        Ok(config)
    }

    fn resolve_endpoint(&self) -> Result<String> {
        if let Some(url) = &self.url {
            return Ok(url.clone());
        }

        self.config
            .as_ref()
            .and_then(|values| values.get("fs.defaultFS"))
            .map(|value| endpoint_from_default_fs(value))
            .transpose()?
            .ok_or_else(|| {
                Error::InvalidArgument(
                    "ClientBuilder requires with_url for the Ozone OM endpoint".to_string(),
                )
            })
    }
}

#[derive(Clone)]
pub struct Client {
    inner: Arc<OzoneClient>,
}

impl Client {
    pub fn from_ozone(inner: OzoneClient) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    pub async fn read(&self, volume: &str, bucket: &str, key: &str) -> Result<FileReader> {
        Ok(FileReader::new(
            self.inner.get_file_bytes(volume, bucket, key).await?,
        ))
    }

    pub async fn create(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        write_options: impl AsRef<WriteOptions>,
    ) -> Result<FileWriter> {
        FileWriter::create(
            Arc::clone(&self.inner),
            volume.to_string(),
            bucket.to_string(),
            key.to_string(),
            write_options.as_ref().clone(),
        )
        .await
    }

    pub async fn get_file_info(&self, volume: &str, bucket: &str, key: &str) -> Result<FileStatus> {
        self.inner.get_file_status(volume, bucket, key).await
    }

    /// Collects every matching status into memory.
    ///
    /// Prefer [`Client::list_status_iter`] for large directories so entries are
    /// fetched and converted a page at a time.
    pub async fn list_status(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        recursive: bool,
    ) -> Result<Vec<FileStatus>> {
        self.inner.list_status(volume, bucket, key, recursive).await
    }

    pub(crate) async fn list_status_page(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        recursive: bool,
        start_key: &str,
    ) -> Result<ListStatusPage> {
        self.inner
            .list_status_page(
                volume,
                bucket,
                key,
                recursive,
                start_key,
                LIST_STATUS_PAGE_SIZE,
            )
            .await
    }

    /// Returns a lazy paginated status iterator.
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
        self.inner
            .create_directory(volume, bucket, key, create_parent)
            .await
    }

    pub async fn delete(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        recursive: bool,
    ) -> Result<bool> {
        self.inner
            .delete_key_with_recursive(volume, bucket, key, recursive)
            .await?;
        Ok(true)
    }

    pub async fn set_times(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        mtime: u64,
        atime: u64,
    ) -> Result<()> {
        self.inner
            .set_times(volume, bucket, key, mtime, atime)
            .await
    }

    pub async fn modify_acl_entries(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        acl_spec: Vec<AclEntry>,
    ) -> Result<()> {
        self.inner.add_acls(volume, bucket, key, acl_spec).await
    }

    pub async fn remove_acl_entries(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        acl_spec: Vec<AclEntry>,
    ) -> Result<()> {
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

    pub async fn set_acl(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        acl_spec: Vec<AclEntry>,
    ) -> Result<()> {
        self.inner.set_acls(volume, bucket, key, acl_spec).await
    }

    pub async fn get_acl_status(&self, volume: &str, bucket: &str, key: &str) -> Result<AclStatus> {
        self.inner.get_acl_status(volume, bucket, key).await
    }

    pub async fn append(&self, _volume: &str, _bucket: &str, _key: &str) -> Result<FileWriter> {
        unsupported(
            "Client::append",
            "Ozone append is not implemented by this compatibility layer",
        )
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
        unsupported(
            "Client::rename",
            "Ozone rename is not implemented by this compatibility layer",
        )
    }

    pub async fn set_owner(
        &self,
        _volume: &str,
        _bucket: &str,
        _key: &str,
        _owner: Option<&str>,
        _group: Option<&str>,
    ) -> Result<()> {
        unsupported(
            "Client::set_owner",
            "Ozone does not expose HDFS owner/group mutation here",
        )
    }

    pub async fn set_permission(
        &self,
        _volume: &str,
        _bucket: &str,
        _key: &str,
        _permission: u32,
    ) -> Result<()> {
        unsupported(
            "Client::set_permission",
            "Ozone does not expose HDFS POSIX permission mutation here",
        )
    }

    pub async fn set_replication(
        &self,
        _volume: &str,
        _bucket: &str,
        _key: &str,
        _replication: u32,
    ) -> Result<bool> {
        unsupported(
            "Client::set_replication",
            "Ozone replication mutation is not implemented by this compatibility layer",
        )
    }

    pub async fn get_content_summary(
        &self,
        _volume: &str,
        _bucket: &str,
        _key: &str,
    ) -> Result<ContentSummary> {
        unsupported(
            "Client::get_content_summary",
            "Ozone content summary is not implemented by this compatibility layer",
        )
    }

    pub async fn glob_status(
        &self,
        _volume: &str,
        _bucket: &str,
        _pattern: &str,
    ) -> Result<Vec<FileStatus>> {
        unsupported(
            "Client::glob_status",
            "Hadoop-style globbing is not implemented by this compatibility layer",
        )
    }
}

fn unsupported<T>(method: &str, reason: &str) -> Result<T> {
    Err(Error::Unsupported(format!("{method}: {reason}")))
}

fn apply_config(config: &mut ClientConfig, key: &str, value: &str) -> Result<()> {
    match key {
        "fs.defaultFS" => {}
        "ozone.chunk.size" => config.chunk_size = parse_usize(key, value)?,
        "ozone.stream.flush.size" => config.stream_flush_size = parse_usize(key, value)?,
        "ozone.stream.window.size" => config.stream_window_size = parse_usize(key, value)?,
        "ozone.read.response.size" => config.read_response_size = parse_u32(key, value)?,
        "ozone.watch.for.commit" => config.watch_for_commit = parse_bool(key, value)?,
        "ozone.max.write.retries" => config.max_write_retries = parse_usize(key, value)?,
        "ozone.enable.put.block.piggybacking" => {
            config.enable_put_block_piggybacking = parse_bool(key, value)?
        }
        "ozone.host.override" => config.host_override = Some(value.to_string()),
        _ => {
            return Err(Error::InvalidArgument(format!(
                "unknown client config key `{key}`"
            )));
        }
    }
    Ok(())
}

fn endpoint_from_default_fs(value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(Error::InvalidArgument(
            "fs.defaultFS must not be empty".to_string(),
        ));
    }

    let Some((scheme, rest)) = value.split_once("://") else {
        return Ok(value.to_string());
    };
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default()
        .rsplit('@')
        .next()
        .unwrap_or_default();
    if authority.is_empty() {
        return Err(Error::InvalidArgument(format!(
            "fs.defaultFS URI `{value}` does not include an authority"
        )));
    }

    if scheme.eq_ignore_ascii_case("o3fs") {
        return Ok(o3fs_om_authority(authority));
    }
    Ok(authority.to_string())
}

fn o3fs_om_authority(authority: &str) -> String {
    let (host, port) = split_authority_port(authority);
    if host.starts_with('[') {
        return authority.to_string();
    }

    let labels = host.split('.').collect::<Vec<_>>();
    if labels.len() < 3 {
        return authority.to_string();
    }
    format!("{}{}", labels[2..].join("."), port)
}

fn split_authority_port(authority: &str) -> (&str, &str) {
    let Some(index) = authority.rfind(':') else {
        return (authority, "");
    };
    if authority[index + 1..].chars().all(|ch| ch.is_ascii_digit()) {
        (&authority[..index], &authority[index..])
    } else {
        (authority, "")
    }
}

fn parse_usize(key: &str, value: &str) -> Result<usize> {
    value
        .parse()
        .map_err(|_| Error::InvalidArgument(format!("invalid usize for `{key}`: {value}")))
}

fn parse_u32(key: &str, value: &str) -> Result<u32> {
    value
        .parse()
        .map_err(|_| Error::InvalidArgument(format!("invalid u32 for `{key}`: {value}")))
}

fn parse_bool(key: &str, value: &str) -> Result<bool> {
    value
        .parse()
        .map_err(|_| Error::InvalidArgument(format!("invalid bool for `{key}`: {value}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_default_fs_o3fs_uri_to_om_endpoint() {
        let builder = ClientBuilder::new().with_config(vec![(
            "fs.defaultFS",
            "o3fs://bucket.volume.om.example.com:9862/path",
        )]);

        assert_eq!(builder.resolve_endpoint().unwrap(), "om.example.com:9862");
    }

    #[test]
    fn resolves_default_fs_ofs_uri_to_authority() {
        let builder = ClientBuilder::new().with_config(vec![(
            "fs.defaultFS",
            "ofs://om.example.com:9862/volume/bucket/key",
        )]);

        assert_eq!(builder.resolve_endpoint().unwrap(), "om.example.com:9862");
    }

    #[test]
    fn leaves_plain_default_fs_endpoint_unchanged() {
        let builder =
            ClientBuilder::new().with_config(vec![("fs.defaultFS", "om.example.com:9862")]);

        assert_eq!(builder.resolve_endpoint().unwrap(), "om.example.com:9862");
    }

    #[test]
    fn explicit_url_takes_precedence_over_default_fs() {
        let builder = ClientBuilder::new()
            .with_url("http://127.0.0.1:9874")
            .with_config(vec![("fs.defaultFS", "ofs://om.example.com:9862/")]);

        assert_eq!(builder.resolve_endpoint().unwrap(), "http://127.0.0.1:9874");
    }
}
