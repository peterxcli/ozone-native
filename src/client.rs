use crate::acl::{key_obj, AclEntry, AclStatus};
use crate::block_writer::BlockWriter;
use crate::datanode::DatanodeClient;
use crate::error::{Error, Result};
use crate::om::{BlockAllocateExcludeList, KeyReplication, OmClient, OpenKeySession};
use crate::proto::hadoop::hdds;
use crate::proto::hadoop::ozone::{self, BasicKeyInfo, BucketLayoutProto};
use crate::ratis::RatisClient;
use crate::status::FileStatus;
use crate::util::{
    key_replication, latest_key_locations, DEFAULT_CHUNK_SIZE, DEFAULT_MAX_WRITE_RETRIES,
    DEFAULT_READ_RESPONSE_SIZE, DEFAULT_STREAM_FLUSH_SIZE, DEFAULT_STREAM_WINDOW_SIZE,
};
use std::collections::{BTreeMap, HashSet};

const FSO_LIST_PAGE_SIZE: u64 = 1024;
pub(crate) const LIST_STATUS_PAGE_SIZE: u64 = 1024;

#[derive(Debug)]
pub(crate) struct ListStatusPage {
    pub statuses: Vec<FileStatus>,
    pub next_start_key: Option<String>,
    pub has_more: bool,
}

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub chunk_size: usize,
    pub stream_flush_size: usize,
    pub stream_window_size: usize,
    pub read_response_size: u32,
    pub watch_for_commit: bool,
    pub max_write_retries: usize,
    pub enable_put_block_piggybacking: bool,
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
            enable_put_block_piggybacking: false,
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
        self.om.delete_key(volume, bucket, key, false).await
    }

    pub(crate) async fn delete_key_with_recursive(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        recursive: bool,
    ) -> Result<()> {
        self.om.delete_key(volume, bucket, key, recursive).await
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

        self.write_open_key_bytes(volume, bucket, key, data, open)
            .await
    }

    pub async fn put_file_bytes(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        data: &[u8],
        recursive: bool,
        overwrite: bool,
        replication_factor: Option<u32>,
    ) -> Result<ozone::KeyInfo> {
        let replication = replication_from_factor(replication_factor)?;
        let open = self
            .om
            .create_file(
                volume,
                bucket,
                key,
                data.len() as u64,
                recursive,
                overwrite,
                &replication,
            )
            .await?;

        self.write_open_key_bytes(volume, bucket, key, data, open)
            .await
    }

    pub(crate) async fn create_file_for_write(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        recursive: bool,
        overwrite: bool,
        replication_factor: Option<u32>,
    ) -> Result<OpenKeySession> {
        let replication = replication_from_factor(replication_factor)?;
        self.om
            .create_file(volume, bucket, key, 0, recursive, overwrite, &replication)
            .await
    }

    pub(crate) async fn allocate_file_block_for_write(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        data_size: u64,
        client_id: u64,
        replication: &KeyReplication,
        exclude: Option<&BlockAllocateExcludeList>,
    ) -> Result<ozone::KeyLocation> {
        self.om
            .allocate_block(
                volume,
                bucket,
                key,
                data_size,
                client_id,
                replication,
                exclude,
            )
            .await
    }

    pub(crate) async fn create_block_writer(
        &self,
        location: &ozone::KeyLocation,
    ) -> Result<BlockWriter> {
        BlockWriter::new(&self.ratis, &self.config, location).await
    }

    pub(crate) async fn commit_open_file(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        data_size: u64,
        client_id: u64,
        locations: Vec<ozone::KeyLocation>,
        replication: &KeyReplication,
    ) -> Result<()> {
        self.om
            .commit_key(
                volume,
                bucket,
                key,
                data_size,
                client_id,
                locations,
                replication,
            )
            .await
    }

    async fn write_open_key_bytes(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        data: &[u8],
        open: OpenKeySession,
    ) -> Result<ozone::KeyInfo> {
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
            let pending_lengths = pending
                .iter()
                .map(|location| location.length)
                .collect::<Vec<_>>();
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

    pub async fn get_file_bytes(&self, volume: &str, bucket: &str, key: &str) -> Result<Vec<u8>> {
        let key_info = self.om.lookup_file(volume, bucket, key).await?;
        self.datanode.read_key_blocks(&key_info).await
    }

    pub async fn get_file_status(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
    ) -> Result<FileStatus> {
        FileStatus::from_ozone(self.om.get_file_status(volume, bucket, key).await?)
    }

    /// Collects every matching status into memory.
    ///
    /// The HDFS-compatible [`crate::Client::list_status_iter`] API uses the
    /// same paginated OM requests without collecting all pages first.
    pub async fn list_status(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        recursive: bool,
    ) -> Result<Vec<FileStatus>> {
        let mut start_key = String::new();
        let mut statuses = Vec::new();
        loop {
            let page = self
                .list_status_page(
                    volume,
                    bucket,
                    key,
                    recursive,
                    &start_key,
                    LIST_STATUS_PAGE_SIZE,
                )
                .await?;
            statuses.extend(page.statuses);
            if !page.has_more {
                break;
            }
            let Some(next_start_key) = page.next_start_key else {
                break;
            };
            start_key = next_start_key;
        }

        Ok(statuses)
    }

    pub(crate) async fn list_status_page(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        recursive: bool,
        start_key: &str,
        num_entries: u64,
    ) -> Result<ListStatusPage> {
        let page = self
            .om
            .list_status(volume, bucket, key, recursive, start_key, num_entries)
            .await?;
        build_list_status_page(page, start_key, num_entries)
    }

    pub async fn create_directory(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        recursive: bool,
    ) -> Result<()> {
        self.om
            .create_directory(volume, bucket, key, recursive)
            .await
    }

    pub async fn set_times(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        mtime: u64,
        atime: u64,
    ) -> Result<()> {
        self.om.set_times(volume, bucket, key, mtime, atime).await
    }

    pub async fn get_acl_status(&self, volume: &str, bucket: &str, key: &str) -> Result<AclStatus> {
        AclStatus::from_ozone(self.om.get_acl(key_obj(volume, bucket, key)).await?)
    }

    pub async fn add_acls(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        acl_spec: Vec<AclEntry>,
    ) -> Result<()> {
        let obj = key_obj(volume, bucket, key);
        for acl in acl_spec {
            if !self.om.add_acl(obj.clone(), acl.to_ozone()).await? {
                return Err(Error::InvalidState(
                    "OM returned false for addAcl".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub async fn remove_acls(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        acl_spec: Vec<AclEntry>,
    ) -> Result<()> {
        let obj = key_obj(volume, bucket, key);
        for acl in acl_spec {
            if !self.om.remove_acl(obj.clone(), acl.to_ozone()).await? {
                return Err(Error::InvalidState(
                    "OM returned false for removeAcl".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub async fn set_acls(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        acl_spec: Vec<AclEntry>,
    ) -> Result<()> {
        let acls = acl_spec.into_iter().map(|acl| acl.to_ozone()).collect();
        if !self.om.set_acl(key_obj(volume, bucket, key), acls).await? {
            return Err(Error::InvalidState(
                "OM returned false for setAcl".to_string(),
            ));
        }
        Ok(())
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

fn should_allocate_new_block(
    pending_block_lengths: &[u64],
    written: usize,
    total_len: usize,
) -> bool {
    let _ = (written, total_len);
    pending_block_lengths.is_empty()
}

pub(crate) fn replication_from_factor(replication: Option<u32>) -> Result<KeyReplication> {
    let Some(factor) = replication else {
        return Ok(KeyReplication::default());
    };

    let factor = match factor {
        1 => hdds::ReplicationFactor::One,
        3 => hdds::ReplicationFactor::Three,
        _ => {
            return Err(Error::InvalidArgument(format!(
                "unsupported replication factor {factor}; Ozone supports 1 or 3"
            )));
        }
    };

    Ok(KeyReplication {
        replication_type: Some(hdds::ReplicationType::Ratis as i32),
        factor: Some(factor as i32),
        ec_replication: None,
    })
}

fn build_list_status_page(
    raw_statuses: Vec<ozone::OzoneFileStatusProto>,
    start_key: &str,
    num_entries: u64,
) -> Result<ListStatusPage> {
    let raw_len = raw_statuses.len();
    let mut statuses = Vec::with_capacity(raw_len);
    let mut next_start_key = None;

    for status in raw_statuses {
        let status_key = status
            .key_info
            .as_ref()
            .map(|key_info| key_info.key_name.clone());
        if let Some(key) = &status_key {
            next_start_key = Some(key.clone());
        }
        if !start_key.is_empty() && status_key.as_deref() == Some(start_key) {
            continue;
        }
        statuses.push(FileStatus::from_ozone(status)?);
    }

    let has_more = raw_len >= num_entries as usize
        && next_start_key
            .as_deref()
            .is_some_and(|next_key| next_key != start_key);

    Ok(ListStatusPage {
        statuses,
        next_start_key,
        has_more,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::hadoop::hdds;

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

    #[test]
    fn maps_write_options_replication_to_ratis_factor() {
        let replication = replication_from_factor(Some(3)).unwrap();

        assert_eq!(
            replication.replication_type,
            Some(hdds::ReplicationType::Ratis as i32)
        );
        assert_eq!(
            replication.factor,
            Some(hdds::ReplicationFactor::Three as i32)
        );
        assert_eq!(replication.ec_replication, None);
    }

    #[test]
    fn leaves_default_replication_unset() {
        let replication = replication_from_factor(None).unwrap();

        assert_eq!(replication.replication_type, None);
        assert_eq!(replication.factor, None);
        assert_eq!(replication.ec_replication, None);
    }

    #[test]
    fn rejects_unsupported_replication_factor() {
        let err = replication_from_factor(Some(2)).unwrap_err();

        assert!(err.to_string().contains("unsupported replication factor"));
    }

    #[test]
    fn defaults_match_ozone_client_write_safety_defaults() {
        let config = ClientConfig::default();

        assert!(!config.enable_put_block_piggybacking);
    }

    #[test]
    fn list_status_page_skips_repeated_start_key() {
        let page = build_list_status_page(
            vec![ozone_status("dir/a"), ozone_status("dir/b")],
            "dir/a",
            2,
        )
        .unwrap();

        assert_eq!(
            page.statuses
                .iter()
                .map(|status| status.key.as_str())
                .collect::<Vec<_>>(),
            vec!["dir/b"]
        );
        assert_eq!(page.next_start_key.as_deref(), Some("dir/b"));
        assert!(page.has_more);
    }

    #[test]
    fn list_status_page_marks_short_page_finished() {
        let page = build_list_status_page(
            vec![ozone_status("dir/a"), ozone_status("dir/b")],
            "dir/a",
            3,
        )
        .unwrap();

        assert_eq!(
            page.statuses
                .iter()
                .map(|status| status.key.as_str())
                .collect::<Vec<_>>(),
            vec!["dir/b"]
        );
        assert_eq!(page.next_start_key.as_deref(), Some("dir/b"));
        assert!(!page.has_more);
    }

    fn ozone_status(key: &str) -> ozone::OzoneFileStatusProto {
        ozone::OzoneFileStatusProto {
            key_info: Some(ozone::KeyInfo {
                volume_name: "vol".to_string(),
                bucket_name: "bucket".to_string(),
                key_name: key.to_string(),
                data_size: 0,
                r#type: hdds::ReplicationType::Ratis as i32,
                factor: Some(hdds::ReplicationFactor::Three as i32),
                key_location_list: Vec::new(),
                creation_time: 0,
                modification_time: 0,
                latest_version: None,
                metadata: Vec::new(),
                file_encryption_info: None,
                acls: Vec::new(),
                object_id: None,
                update_id: None,
                parent_id: None,
                ec_replication_config: None,
                file_checksum: None,
                is_file: Some(true),
                owner_name: None,
                tags: Vec::new(),
                expected_data_generation: None,
            }),
            block_size: None,
            is_directory: Some(false),
        }
    }
}
