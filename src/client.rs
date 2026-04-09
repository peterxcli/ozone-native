use crate::block_writer::BlockWriter;
use crate::datanode::DatanodeClient;
use crate::error::Result;
use crate::om::{BlockAllocateExcludeList, KeyReplication, OmClient};
use crate::proto::hadoop::ozone::{self, BasicKeyInfo, BucketLayoutProto};
use crate::ratis::RatisClient;
use crate::util::{
    key_replication, latest_key_locations, DEFAULT_CHUNK_SIZE, DEFAULT_MAX_WRITE_RETRIES,
    DEFAULT_READ_RESPONSE_SIZE, DEFAULT_STREAM_FLUSH_SIZE, DEFAULT_STREAM_WINDOW_SIZE,
};
use std::collections::{BTreeMap, HashSet};

const FSO_LIST_PAGE_SIZE: u64 = 1024;

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub chunk_size: usize,
    pub stream_flush_size: usize,
    pub stream_window_size: usize,
    pub read_response_size: u32,
    pub watch_for_commit: bool,
    pub max_write_retries: usize,
    pub enable_put_block_piggybacking: bool,
    pub enable_incremental_chunk_list: bool,
    pub host_override: Option<String>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
            stream_flush_size: DEFAULT_STREAM_FLUSH_SIZE,
            stream_window_size: DEFAULT_STREAM_WINDOW_SIZE,
            read_response_size: DEFAULT_READ_RESPONSE_SIZE,
            watch_for_commit: true,
            max_write_retries: DEFAULT_MAX_WRITE_RETRIES,
            enable_put_block_piggybacking: true,
            enable_incremental_chunk_list: true,
            host_override: None,
        }
    }
}

pub struct OzoneClient {
    om: OmClient,
    ratis: RatisClient,
    datanode: DatanodeClient,
    config: ClientConfig,
}

impl OzoneClient {
    pub async fn connect(om_endpoint: &str) -> Result<Self> {
        Self::connect_with_config(om_endpoint, ClientConfig::default()).await
    }

    pub async fn connect_with_config(om_endpoint: &str, config: ClientConfig) -> Result<Self> {
        let om = OmClient::connect(om_endpoint).await?;
        let datanode = DatanodeClient::new(config.read_response_size, config.host_override.clone());
        Ok(Self {
            om,
            ratis: RatisClient::new(config.host_override.clone()),
            datanode,
            config,
        })
    }

    pub async fn create_volume(&self, volume: &str, owner: &str, admin: &str) -> Result<()> {
        self.om.create_volume(volume, owner, admin).await
    }

    pub async fn info_volume(&self, volume: &str) -> Result<ozone::VolumeInfo> {
        self.om.info_volume(volume).await
    }

    pub async fn list_volumes(&self, prefix: Option<&str>) -> Result<Vec<ozone::VolumeInfo>> {
        self.om.list_volumes(prefix).await
    }

    pub async fn delete_volume(&self, volume: &str) -> Result<()> {
        self.om.delete_volume(volume).await
    }

    pub async fn create_bucket(&self, volume: &str, bucket: &str) -> Result<()> {
        self.om.create_bucket(volume, bucket).await
    }

    pub async fn info_bucket(&self, volume: &str, bucket: &str) -> Result<ozone::BucketInfo> {
        self.om.info_bucket(volume, bucket).await
    }

    pub async fn list_buckets(
        &self,
        volume: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<ozone::BucketInfo>> {
        self.om.list_buckets(volume, prefix).await
    }

    pub async fn delete_bucket(&self, volume: &str, bucket: &str) -> Result<()> {
        self.om.delete_bucket(volume, bucket).await
    }

    pub async fn list_keys(
        &self,
        volume: &str,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<ozone::KeyInfo>> {
        let bucket_info = self.om.info_bucket(volume, bucket).await?;
        let bucket_layout = bucket_info
            .bucket_layout
            .and_then(|layout| BucketLayoutProto::try_from(layout).ok());
        if bucket_layout == Some(BucketLayoutProto::FileSystemOptimized) {
            return self.list_keys_fso(volume, bucket, prefix).await;
        }
        self.om.list_keys(volume, bucket, prefix).await
    }

    pub async fn get_key_info(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
    ) -> Result<ozone::KeyInfo> {
        self.om.get_key_info(volume, bucket, key).await
    }

    pub async fn delete_key(&self, volume: &str, bucket: &str, key: &str) -> Result<()> {
        self.om.delete_key(volume, bucket, key).await
    }

    pub async fn put_key_bytes(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        data: &[u8],
    ) -> Result<ozone::KeyInfo> {
        let open = self
            .om
            .create_key(
                volume,
                bucket,
                key,
                data.len() as u64,
                &KeyReplication::default(),
            )
            .await?;

        let (replication_type, factor, ec_replication) = key_replication(&open.key_info);
        let replication = KeyReplication {
            replication_type,
            factor,
            ec_replication,
        };

        let mut pending = latest_key_locations(&open.key_info);
        let mut committed = Vec::new();
        let mut written = 0usize;

        while written < data.len() {
            let pending_lengths = pending.iter().map(|location| location.length).collect::<Vec<_>>();
            if should_allocate_new_block(&pending_lengths, written, data.len()) {
                pending.push(
                    self.om
                        .allocate_block(
                            volume,
                            bucket,
                            key,
                            data.len() as u64,
                            open.id,
                            &replication,
                            None,
                        )
                        .await?,
                );
            }

            let location = pending.remove(0);
            let capacity = if location.length == 0 {
                data.len() - written
            } else {
                location.length as usize
            };
            let block_len = capacity.min(data.len() - written);
            let block_data = &data[written..written + block_len];
            let mut current_location = location;
            let mut exclude = BlockAllocateExcludeList::default();
            let mut attempt = 0usize;

            loop {
                let writer = BlockWriter::new(&self.ratis, &self.config, &current_location).await?;
                match writer.write_all(block_data).await {
                    Ok(locations) => {
                        committed.extend(locations);
                        break;
                    }
                    Err(_err) if attempt < self.config.max_write_retries => {
                        if let Some(pipeline) = current_location.pipeline.as_ref() {
                            exclude.pipeline_ids.push(pipeline.id.clone());
                        }
                        attempt += 1;
                        current_location = self
                            .om
                            .allocate_block(
                                volume,
                                bucket,
                                key,
                                data.len() as u64,
                                open.id,
                                &replication,
                                Some(&exclude),
                            )
                            .await?;
                        continue;
                    }
                    Err(err) => return Err(err),
                }
            }
            written += block_len;
        }

        self.om
            .commit_key(
                volume,
                bucket,
                key,
                data.len() as u64,
                open.id,
                committed,
                &replication,
            )
            .await?;

        self.om.get_key_info(volume, bucket, key).await
    }

    pub async fn get_key_bytes(&self, volume: &str, bucket: &str, key: &str) -> Result<Vec<u8>> {
        let key_info = self.om.lookup_key(volume, bucket, key).await?;
        self.datanode.read_key_blocks(&key_info).await
    }
    async fn list_keys_fso(
        &self,
        volume: &str,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<ozone::KeyInfo>> {
        let requested_prefix = prefix.unwrap_or("");
        let mut stack = vec![String::new()];
        let mut queued_directories = HashSet::from([String::new()]);
        let mut keys = BTreeMap::new();

        while let Some(directory) = stack.pop() {
            let mut start_key = String::new();
            loop {
                let statuses = self
                    .om
                    .list_status_light(
                        volume,
                        bucket,
                        &directory,
                        &start_key,
                        FSO_LIST_PAGE_SIZE,
                        true,
                    )
                    .await?;
                if statuses.is_empty() {
                    break;
                }

                let page_len = statuses.len();
                let mut last_key_name = None;

                for status in statuses {
                    let Some(basic) = status.basic_key_info.clone() else {
                        continue;
                    };
                    let key_name = basic.key_name.clone().unwrap_or_default();
                    if key_name.is_empty() {
                        continue;
                    }
                    last_key_name = Some(key_name.clone());

                    // OM repeats the start key at the beginning of the next page.
                    if !start_key.is_empty() && key_name == start_key {
                        continue;
                    }

                    let is_directory = status
                        .is_directory
                        .unwrap_or(!basic.is_file.unwrap_or(true));
                    if is_directory
                        && should_descend_fso_directory(&key_name, requested_prefix)
                        && queued_directories.insert(key_name.clone())
                    {
                        stack.push(key_name.clone());
                    }

                    let key_info = basic_key_info_to_key_info(volume, bucket, basic, is_directory);
                    if matches_fso_prefix(requested_prefix, &key_name, &key_info.key_name) {
                        keys.entry(key_info.key_name.clone()).or_insert(key_info);
                    }
                }

                if page_len < FSO_LIST_PAGE_SIZE as usize {
                    break;
                }

                let Some(next_start_key) = last_key_name else {
                    break;
                };
                if next_start_key == start_key {
                    break;
                }
                start_key = next_start_key;
            }
        }

        Ok(keys.into_values().collect())
    }
}

fn basic_key_info_to_key_info(
    volume: &str,
    bucket: &str,
    basic: BasicKeyInfo,
    is_directory: bool,
) -> ozone::KeyInfo {
    let key_name = basic.key_name.unwrap_or_default();
    let rendered_key_name = if is_directory && !key_name.ends_with('/') {
        format!("{key_name}/")
    } else {
        key_name
    };

    ozone::KeyInfo {
        volume_name: volume.to_string(),
        bucket_name: bucket.to_string(),
        key_name: rendered_key_name,
        data_size: basic.data_size.unwrap_or_default(),
        r#type: basic
            .r#type
            .unwrap_or(crate::proto::hadoop::hdds::ReplicationType::Ratis as i32),
        factor: basic.factor,
        key_location_list: Vec::new(),
        creation_time: basic.creation_time.unwrap_or_default(),
        modification_time: basic.modification_time.unwrap_or_default(),
        latest_version: None,
        metadata: Vec::new(),
        file_encryption_info: None,
        acls: Vec::new(),
        object_id: None,
        update_id: None,
        parent_id: None,
        ec_replication_config: basic.ec_replication_config,
        file_checksum: None,
        is_file: Some(!is_directory && basic.is_file.unwrap_or(true)),
        owner_name: basic.owner_name,
        tags: Vec::new(),
        expected_data_generation: None,
        expected_e_tag: basic.e_tag,
    }
}

fn matches_fso_prefix(requested_prefix: &str, raw_key_name: &str, rendered_key_name: &str) -> bool {
    requested_prefix.is_empty()
        || raw_key_name.starts_with(requested_prefix)
        || rendered_key_name.starts_with(requested_prefix)
}

fn should_descend_fso_directory(directory: &str, requested_prefix: &str) -> bool {
    requested_prefix.is_empty()
        || directory.is_empty()
        || directory.starts_with(requested_prefix)
        || requested_prefix.starts_with(directory)
}

fn should_allocate_new_block(pending_block_lengths: &[u64], written: usize, total_len: usize) -> bool {
    let _ = (written, total_len);
    pending_block_lengths.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_fso_prefix_handles_directory_rendering() {
        assert!(matches_fso_prefix("dir/", "dir", "dir/"));
        assert!(matches_fso_prefix("dir", "dir/file.txt", "dir/file.txt"));
        assert!(!matches_fso_prefix("other", "dir/file.txt", "dir/file.txt"));
    }

    #[test]
    fn descends_into_relevant_fso_directories() {
        assert!(should_descend_fso_directory("dir", "dir/file.txt"));
        assert!(should_descend_fso_directory("dir/sub", "dir"));
        assert!(should_descend_fso_directory("test", "te"));
        assert!(!should_descend_fso_directory("other", "dir/file.txt"));
    }

    #[test]
    fn allocates_new_block_when_preallocated_list_is_empty() {
        assert!(should_allocate_new_block(&[], 1024, 1024));
    }

    #[test]
    fn reuses_preallocated_block_when_available() {
        assert!(!should_allocate_new_block(&[8], 0, 8));
    }
}
