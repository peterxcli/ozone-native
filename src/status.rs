use crate::error::{Error, Result};
use crate::proto::hadoop::ozone;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStatus {
    pub volume: String,
    pub bucket: String,
    pub key: String,
    pub length: usize,
    pub isdir: bool,
    pub permission: u16,
    pub owner: String,
    pub group: String,
    pub modification_time: u64,
    pub access_time: u64,
    pub replication: Option<u32>,
    pub blocksize: Option<u64>,
}

impl FileStatus {
    pub(crate) fn from_ozone(status: ozone::OzoneFileStatusProto) -> Result<Self> {
        let key_info = status
            .key_info
            .ok_or(Error::MissingField("ozone_file_status.key_info"))?;
        Ok(Self {
            volume: key_info.volume_name,
            bucket: key_info.bucket_name,
            key: key_info.key_name,
            length: key_info.data_size as usize,
            isdir: status
                .is_directory
                .unwrap_or(!key_info.is_file.unwrap_or(true)),
            permission: 0,
            owner: key_info.owner_name.unwrap_or_default(),
            group: String::new(),
            modification_time: key_info.modification_time,
            access_time: 0,
            replication: key_info.factor.map(|factor| factor as u32),
            blocksize: status.block_size,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentSummary {
    pub length: u64,
    pub file_count: u64,
    pub directory_count: u64,
    pub quota: u64,
    pub space_consumed: u64,
    pub space_quota: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::hadoop::{hdds, ozone};

    fn key_info(is_file: Option<bool>) -> ozone::KeyInfo {
        ozone::KeyInfo {
            volume_name: "vol".to_string(),
            bucket_name: "bucket".to_string(),
            key_name: "dir/file.txt".to_string(),
            data_size: 42,
            r#type: hdds::ReplicationType::Ratis as i32,
            factor: Some(hdds::ReplicationFactor::Three as i32),
            key_location_list: Vec::new(),
            creation_time: 10,
            modification_time: 20,
            latest_version: None,
            metadata: Vec::new(),
            file_encryption_info: None,
            acls: Vec::new(),
            object_id: None,
            update_id: None,
            parent_id: None,
            ec_replication_config: None,
            file_checksum: None,
            is_file,
            owner_name: Some("owner".to_string()),
            tags: Vec::new(),
            expected_data_generation: None,
            expected_e_tag: None,
        }
    }

    #[test]
    fn converts_ozone_file_status_to_compat_status() {
        let status = FileStatus::from_ozone(ozone::OzoneFileStatusProto {
            key_info: Some(key_info(Some(true))),
            block_size: Some(128),
            is_directory: Some(false),
        })
        .unwrap();

        assert_eq!(status.volume, "vol");
        assert_eq!(status.bucket, "bucket");
        assert_eq!(status.key, "dir/file.txt");
        assert_eq!(status.length, 42);
        assert!(!status.isdir);
        assert_eq!(status.owner, "owner");
        assert_eq!(status.group, "");
        assert_eq!(status.permission, 0);
        assert_eq!(status.modification_time, 20);
        assert_eq!(status.access_time, 0);
        assert_eq!(
            status.replication,
            Some(hdds::ReplicationFactor::Three as u32)
        );
        assert_eq!(status.blocksize, Some(128));
    }

    #[test]
    fn converts_ozone_directory_status_to_compat_status() {
        let status = FileStatus::from_ozone(ozone::OzoneFileStatusProto {
            key_info: Some(key_info(Some(false))),
            block_size: None,
            is_directory: Some(true),
        })
        .unwrap();

        assert!(status.isdir);
        assert_eq!(status.blocksize, None);
    }
}
