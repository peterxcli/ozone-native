use crate::error::{Error, Result};
use crate::proto::hadoop::ozone::ozone_manager_service_client::OzoneManagerServiceClient;
use crate::proto::hadoop::{hdds, ozone};
use crate::util::{new_trace_id, normalize_endpoint, CLIENT_VERSION, MAX_GRPC_MESSAGE_SIZE};
use tonic::transport::Channel;
use uuid::Uuid;

#[derive(Clone)]
pub struct OmClient {
    channel: Channel,
    client_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::hadoop::hdds;

    #[test]
    fn file_key_args_sets_recursive_and_replication_fields() {
        let args = file_key_args(
            "vol",
            "bucket",
            "dir/file.txt",
            Some(123),
            &KeyReplication {
                replication_type: Some(hdds::ReplicationType::Ratis as i32),
                factor: Some(hdds::ReplicationFactor::Three as i32),
                ec_replication: None,
            },
            Some(true),
        );

        assert_eq!(args.volume_name, "vol");
        assert_eq!(args.bucket_name, "bucket");
        assert_eq!(args.key_name, "dir/file.txt");
        assert_eq!(args.data_size, Some(123));
        assert_eq!(args.r#type, Some(hdds::ReplicationType::Ratis as i32));
        assert_eq!(args.factor, Some(hdds::ReplicationFactor::Three as i32));
        assert_eq!(args.recursive, Some(true));
        assert_eq!(args.sort_datanodes, Some(true));
    }

    #[test]
    fn delete_key_args_sets_recursive_flag() {
        let args = delete_key_args("vol", "bucket", "dir", true);

        assert_eq!(args.volume_name, "vol");
        assert_eq!(args.bucket_name, "bucket");
        assert_eq!(args.key_name, "dir");
        assert_eq!(args.recursive, Some(true));
    }
}

#[derive(Clone, Debug)]
pub struct OpenKeySession {
    pub id: u64,
    #[allow(dead_code)]
    pub open_version: Option<u64>,
    pub key_info: ozone::KeyInfo,
}

#[derive(Clone, Debug, Default)]
pub struct KeyReplication {
    pub replication_type: Option<i32>,
    pub factor: Option<i32>,
    pub ec_replication: Option<hdds::EcReplicationConfig>,
}

#[derive(Clone, Debug, Default)]
pub struct BlockAllocateExcludeList {
    pub datanodes: Vec<String>,
    pub container_ids: Vec<i64>,
    pub pipeline_ids: Vec<hdds::PipelineId>,
}

fn file_key_args(
    volume: &str,
    bucket: &str,
    key: &str,
    data_size: Option<u64>,
    replication: &KeyReplication,
    recursive: Option<bool>,
) -> ozone::KeyArgs {
    ozone::KeyArgs {
        volume_name: volume.to_string(),
        bucket_name: bucket.to_string(),
        key_name: key.to_string(),
        data_size,
        r#type: replication.replication_type,
        factor: replication.factor,
        key_locations: Vec::new(),
        is_multipart_key: None,
        multipart_upload_id: None,
        multipart_number: None,
        metadata: Vec::new(),
        acls: Vec::new(),
        modification_time: None,
        sort_datanodes: Some(true),
        file_encryption_info: None,
        latest_version_location: None,
        recursive,
        head_op: None,
        ec_replication_config: replication.ec_replication.clone(),
        force_update_container_cache_from_scm: None,
        owner_name: None,
        tags: Vec::new(),
        expected_data_generation: None,
        expected_e_tag: None,
    }
}

fn delete_key_args(volume: &str, bucket: &str, key: &str, recursive: bool) -> ozone::KeyArgs {
    ozone::KeyArgs {
        volume_name: volume.to_string(),
        bucket_name: bucket.to_string(),
        key_name: key.to_string(),
        data_size: None,
        r#type: None,
        factor: None,
        key_locations: Vec::new(),
        is_multipart_key: None,
        multipart_upload_id: None,
        multipart_number: None,
        metadata: Vec::new(),
        acls: Vec::new(),
        modification_time: None,
        sort_datanodes: None,
        file_encryption_info: None,
        latest_version_location: None,
        recursive: Some(recursive),
        head_op: None,
        ec_replication_config: None,
        force_update_container_cache_from_scm: None,
        owner_name: None,
        tags: Vec::new(),
        expected_data_generation: None,
        expected_e_tag: None,
    }
}

impl OmClient {
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let channel = Channel::from_shared(normalize_endpoint(endpoint))
            .map_err(|e| Error::InvalidState(format!("invalid OM endpoint: {e}")))?
            .connect()
            .await?;

        Ok(Self {
            channel,
            client_id: Uuid::new_v4().to_string(),
        })
    }

    #[allow(deprecated)]
    fn request(&self, cmd_type: ozone::Type) -> Box<ozone::OmRequest> {
        Box::new(ozone::OmRequest {
            cmd_type: cmd_type as i32,
            trace_id: Some(new_trace_id()),
            client_id: self.client_id.clone(),
            user_info: None,
            version: Some(CLIENT_VERSION),
            layout_version: None,
            read_consistency_hint: None,
            create_volume_request: None,
            set_volume_property_request: None,
            check_volume_access_request: None,
            info_volume_request: None,
            delete_volume_request: None,
            list_volume_request: None,
            create_bucket_request: None,
            info_bucket_request: None,
            set_bucket_property_request: None,
            delete_bucket_request: None,
            list_buckets_request: None,
            create_key_request: None,
            lookup_key_request: None,
            rename_key_request: None,
            delete_key_request: None,
            list_keys_request: None,
            commit_key_request: None,
            allocate_block_request: None,
            delete_keys_request: None,
            rename_keys_request: None,
            delete_open_keys_request: None,
            initiate_multi_part_upload_request: None,
            commit_multi_part_upload_request: None,
            complete_multi_part_upload_request: None,
            abort_multi_part_upload_request: None,
            get_s3_secret_request: None,
            list_multipart_upload_parts_request: None,
            service_list_request: None,
            db_updates_request: None,
            finalize_upgrade_request: None,
            finalize_upgrade_progress_request: None,
            prepare_request: None,
            prepare_status_request: None,
            cancel_prepare_request: None,
            get_delegation_token_request: None,
            renew_delegation_token_request: None,
            cancel_delegation_token_request: None,
            update_get_delegation_token_request: None,
            updated_renew_delegation_token_request: None,
            get_file_status_request: None,
            create_directory_request: None,
            create_file_request: None,
            lookup_file_request: None,
            list_status_request: None,
            add_acl_request: None,
            remove_acl_request: None,
            set_acl_request: None,
            get_acl_request: None,
            purge_keys_request: None,
            update_get_s3_secret_request: None,
            list_multipart_uploads_request: None,
            list_trash_request: None,
            recover_trash_request: None,
            revoke_s3_secret_request: None,
            purge_paths_request: None,
            purge_directories_request: None,
            s3_authentication: None,
            create_tenant_request: None,
            delete_tenant_request: None,
            list_tenant_request: None,
            tenant_get_user_info_request: None,
            tenant_assign_user_access_id_request: None,
            tenant_revoke_user_access_id_request: None,
            tenant_assign_admin_request: None,
            tenant_revoke_admin_request: None,
            get_s3_volume_context_request: None,
            tenant_list_user_request: None,
            set_s3_secret_request: None,
            set_ranger_service_version_request: None,
            ranger_bg_sync_request: None,
            echo_rpc_request: None,
            get_key_info_request: None,
            create_snapshot_request: None,
            list_snapshot_request: None,
            snapshot_diff_request: None,
            delete_snapshot_request: None,
            snapshot_move_deleted_keys_request: None,
            transfer_om_leadership_request: None,
            snapshot_purge_request: None,
            recover_lease_request: None,
            set_times_request: None,
            refetch_secret_key_request: None,
            list_snapshot_diff_job_request: None,
            cancel_snapshot_diff_request: None,
            submit_snapshot_diff_request: None,
            set_safe_mode_request: None,
            print_compaction_log_dag_request: None,
            multipart_uploads_expired_abort_request: None,
            set_snapshot_property_request: None,
            snapshot_info_request: None,
            rename_snapshot_request: None,
            list_open_files_request: None,
            quota_repair_request: None,
            get_quota_repair_status_request: None,
            start_quota_repair_request: None,
            snapshot_move_table_keys_request: None,
            get_object_tagging_request: None,
            put_object_tagging_request: None,
            delete_object_tagging_request: None,
            set_snapshot_property_requests: Vec::new(),
        })
    }

    async fn submit(&self, request: Box<ozone::OmRequest>) -> Result<ozone::OmResponse> {
        let mut client = OzoneManagerServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE);

        let response = client.submit_request(*request).await?.into_inner();
        self.ensure_success(response)
    }

    fn ensure_success(&self, response: ozone::OmResponse) -> Result<ozone::OmResponse> {
        let status =
            ozone::Status::try_from(response.status).unwrap_or(ozone::Status::InternalError);
        if status != ozone::Status::Ok || response.success == Some(false) {
            return Err(Error::Om {
                status,
                message: response
                    .message
                    .unwrap_or_else(|| "OM request failed without error message".to_string()),
            });
        }
        Ok(response)
    }

    pub async fn create_volume(&self, volume: &str, owner: &str, admin: &str) -> Result<()> {
        let request = ozone::CreateVolumeRequest {
            volume_info: ozone::VolumeInfo {
                admin_name: admin.to_string(),
                owner_name: owner.to_string(),
                volume: volume.to_string(),
                quota_in_bytes: None,
                metadata: Vec::new(),
                volume_acls: Vec::new(),
                creation_time: None,
                object_id: None,
                update_id: None,
                modification_time: None,
                quota_in_namespace: None,
                used_namespace: None,
                ref_count: None,
            },
        };
        let mut om = self.request(ozone::Type::CreateVolume);
        om.create_volume_request = Some(request);
        self.submit(om).await?;
        Ok(())
    }

    pub async fn info_volume(&self, volume: &str) -> Result<ozone::VolumeInfo> {
        let mut om = self.request(ozone::Type::InfoVolume);
        om.info_volume_request = Some(ozone::InfoVolumeRequest {
            volume_name: volume.to_string(),
        });
        let response = self.submit(om).await?;
        response
            .info_volume_response
            .and_then(|r| r.volume_info)
            .ok_or(Error::MissingField("info_volume_response.volume_info"))
    }

    pub async fn list_volumes(&self, prefix: Option<&str>) -> Result<Vec<ozone::VolumeInfo>> {
        let mut om = self.request(ozone::Type::ListVolume);
        om.list_volume_request = Some(ozone::ListVolumeRequest {
            scope: ozone::list_volume_request::Scope::VolumesByCluster as i32,
            user_name: None,
            prefix: prefix.map(ToOwned::to_owned),
            prev_key: None,
            max_keys: Some(1024),
        });
        let response = self.submit(om).await?;
        Ok(response
            .list_volume_response
            .map(|r| r.volume_info)
            .unwrap_or_default())
    }

    pub async fn delete_volume(&self, volume: &str) -> Result<()> {
        let mut om = self.request(ozone::Type::DeleteVolume);
        om.delete_volume_request = Some(ozone::DeleteVolumeRequest {
            volume_name: volume.to_string(),
        });
        self.submit(om).await?;
        Ok(())
    }

    pub async fn create_bucket(&self, volume: &str, bucket: &str) -> Result<()> {
        let request = ozone::CreateBucketRequest {
            bucket_info: ozone::BucketInfo {
                volume_name: volume.to_string(),
                bucket_name: bucket.to_string(),
                acls: Vec::new(),
                is_version_enabled: false,
                storage_type: hdds::StorageTypeProto::Disk as i32,
                creation_time: None,
                metadata: Vec::new(),
                beinfo: None,
                object_id: None,
                update_id: None,
                modification_time: None,
                source_volume: None,
                source_bucket: None,
                used_bytes: None,
                quota_in_bytes: None,
                quota_in_namespace: None,
                used_namespace: None,
                bucket_layout: None,
                owner: None,
                default_replication_config: None,
                snapshot_used_bytes: None,
                snapshot_used_namespace: None,
            },
        };
        let mut om = self.request(ozone::Type::CreateBucket);
        om.create_bucket_request = Some(request);
        self.submit(om).await?;
        Ok(())
    }

    pub async fn info_bucket(&self, volume: &str, bucket: &str) -> Result<ozone::BucketInfo> {
        let mut om = self.request(ozone::Type::InfoBucket);
        om.info_bucket_request = Some(ozone::InfoBucketRequest {
            volume_name: volume.to_string(),
            bucket_name: bucket.to_string(),
        });
        let response = self.submit(om).await?;
        response
            .info_bucket_response
            .and_then(|r| r.bucket_info)
            .ok_or(Error::MissingField("info_bucket_response.bucket_info"))
    }

    pub async fn list_buckets(
        &self,
        volume: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<ozone::BucketInfo>> {
        let mut om = self.request(ozone::Type::ListBuckets);
        om.list_buckets_request = Some(ozone::ListBucketsRequest {
            volume_name: volume.to_string(),
            start_key: None,
            prefix: prefix.map(ToOwned::to_owned),
            count: Some(1024),
            has_snapshot: None,
        });
        let response = self.submit(om).await?;
        Ok(response
            .list_buckets_response
            .map(|r| r.bucket_info)
            .unwrap_or_default())
    }

    pub async fn delete_bucket(&self, volume: &str, bucket: &str) -> Result<()> {
        let mut om = self.request(ozone::Type::DeleteBucket);
        om.delete_bucket_request = Some(ozone::DeleteBucketRequest {
            volume_name: volume.to_string(),
            bucket_name: bucket.to_string(),
        });
        self.submit(om).await?;
        Ok(())
    }

    pub async fn create_key(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        data_size: u64,
        replication: &KeyReplication,
    ) -> Result<OpenKeySession> {
        let mut key_args = ozone::KeyArgs {
            volume_name: volume.to_string(),
            bucket_name: bucket.to_string(),
            key_name: key.to_string(),
            data_size: Some(data_size),
            r#type: replication.replication_type,
            factor: replication.factor,
            key_locations: Vec::new(),
            is_multipart_key: None,
            multipart_upload_id: None,
            multipart_number: None,
            metadata: Vec::new(),
            acls: Vec::new(),
            modification_time: None,
            sort_datanodes: Some(true),
            file_encryption_info: None,
            latest_version_location: None,
            recursive: None,
            head_op: None,
            ec_replication_config: replication.ec_replication.clone(),
            force_update_container_cache_from_scm: None,
            owner_name: None,
            tags: Vec::new(),
            expected_data_generation: None,
            expected_e_tag: None,
        };

        if replication.replication_type.is_none() && replication.ec_replication.is_none() {
            key_args.r#type = None;
            key_args.factor = None;
        }

        let mut om = self.request(ozone::Type::CreateKey);
        om.create_key_request = Some(ozone::CreateKeyRequest {
            key_args,
            client_id: None,
        });
        let response = self.submit(om).await?;
        let key_response = response
            .create_key_response
            .ok_or(Error::MissingField("create_key_response"))?;

        Ok(OpenKeySession {
            id: key_response
                .id
                .ok_or(Error::MissingField("create_key_response.id"))?,
            open_version: key_response.open_version,
            key_info: key_response
                .key_info
                .ok_or(Error::MissingField("create_key_response.key_info"))?,
        })
    }

    pub async fn create_file(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        data_size: u64,
        recursive: bool,
        overwrite: bool,
        replication: &KeyReplication,
    ) -> Result<OpenKeySession> {
        let mut key_args = file_key_args(
            volume,
            bucket,
            key,
            Some(data_size),
            replication,
            Some(recursive),
        );
        if replication.replication_type.is_none() && replication.ec_replication.is_none() {
            key_args.r#type = None;
            key_args.factor = None;
        }

        let mut om = self.request(ozone::Type::CreateFile);
        om.create_file_request = Some(ozone::CreateFileRequest {
            key_args,
            is_recursive: recursive,
            is_overwrite: overwrite,
            client_id: None,
        });
        let response = self.submit(om).await?;
        let file_response = response
            .create_file_response
            .ok_or(Error::MissingField("create_file_response"))?;

        Ok(OpenKeySession {
            id: file_response
                .id
                .ok_or(Error::MissingField("create_file_response.id"))?,
            open_version: file_response.open_version,
            key_info: file_response
                .key_info
                .ok_or(Error::MissingField("create_file_response.key_info"))?,
        })
    }

    pub async fn allocate_block(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        data_size: u64,
        client_id: u64,
        replication: &KeyReplication,
        exclude: Option<&BlockAllocateExcludeList>,
    ) -> Result<ozone::KeyLocation> {
        let mut om = self.request(ozone::Type::AllocateBlock);
        om.allocate_block_request = Some(ozone::AllocateBlockRequest {
            key_args: ozone::KeyArgs {
                volume_name: volume.to_string(),
                bucket_name: bucket.to_string(),
                key_name: key.to_string(),
                data_size: Some(data_size),
                r#type: replication.replication_type,
                factor: replication.factor,
                key_locations: Vec::new(),
                is_multipart_key: None,
                multipart_upload_id: None,
                multipart_number: None,
                metadata: Vec::new(),
                acls: Vec::new(),
                modification_time: None,
                sort_datanodes: Some(true),
                file_encryption_info: None,
                latest_version_location: None,
                recursive: None,
                head_op: None,
                ec_replication_config: replication.ec_replication.clone(),
                force_update_container_cache_from_scm: None,
                owner_name: None,
                tags: Vec::new(),
                expected_data_generation: None,
                expected_e_tag: None,
            },
            client_id,
            exclude_list: exclude.map(|exclude| hdds::ExcludeListProto {
                datanodes: exclude.datanodes.clone(),
                container_ids: exclude.container_ids.clone(),
                pipeline_ids: exclude.pipeline_ids.clone(),
            }),
            key_location: None,
        });
        let response = self.submit(om).await?;
        response
            .allocate_block_response
            .and_then(|r| r.key_location)
            .ok_or(Error::MissingField("allocate_block_response.key_location"))
    }

    pub async fn commit_key(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        data_size: u64,
        client_id: u64,
        locations: Vec<ozone::KeyLocation>,
        replication: &KeyReplication,
    ) -> Result<()> {
        let mut om = self.request(ozone::Type::CommitKey);
        om.commit_key_request = Some(ozone::CommitKeyRequest {
            key_args: ozone::KeyArgs {
                volume_name: volume.to_string(),
                bucket_name: bucket.to_string(),
                key_name: key.to_string(),
                data_size: Some(data_size),
                r#type: replication.replication_type,
                factor: replication.factor,
                key_locations: locations,
                is_multipart_key: None,
                multipart_upload_id: None,
                multipart_number: None,
                metadata: Vec::new(),
                acls: Vec::new(),
                modification_time: None,
                sort_datanodes: Some(true),
                file_encryption_info: None,
                latest_version_location: None,
                recursive: None,
                head_op: None,
                ec_replication_config: replication.ec_replication.clone(),
                force_update_container_cache_from_scm: None,
                owner_name: None,
                tags: Vec::new(),
                expected_data_generation: None,
                expected_e_tag: None,
            },
            client_id,
            hsync: Some(false),
            recovery: Some(false),
        });
        self.submit(om).await?;
        Ok(())
    }

    pub async fn lookup_key(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
    ) -> Result<ozone::KeyInfo> {
        let mut om = self.request(ozone::Type::LookupKey);
        om.lookup_key_request = Some(ozone::LookupKeyRequest {
            key_args: ozone::KeyArgs {
                volume_name: volume.to_string(),
                bucket_name: bucket.to_string(),
                key_name: key.to_string(),
                data_size: None,
                r#type: None,
                factor: None,
                key_locations: Vec::new(),
                is_multipart_key: None,
                multipart_upload_id: None,
                multipart_number: None,
                metadata: Vec::new(),
                acls: Vec::new(),
                modification_time: None,
                sort_datanodes: None,
                file_encryption_info: None,
                latest_version_location: Some(true),
                recursive: None,
                head_op: None,
                ec_replication_config: None,
                force_update_container_cache_from_scm: None,
                owner_name: None,
                tags: Vec::new(),
                expected_data_generation: None,
                expected_e_tag: None,
            },
        });
        let response = self.submit(om).await?;
        response
            .lookup_key_response
            .and_then(|r| r.key_info)
            .ok_or(Error::MissingField("lookup_key_response.key_info"))
    }

    pub async fn lookup_file(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
    ) -> Result<ozone::KeyInfo> {
        let mut om = self.request(ozone::Type::LookupFile);
        om.lookup_file_request = Some(ozone::LookupFileRequest {
            key_args: file_key_args(volume, bucket, key, None, &KeyReplication::default(), None),
        });
        let response = self.submit(om).await?;
        response
            .lookup_file_response
            .and_then(|r| r.key_info)
            .ok_or(Error::MissingField("lookup_file_response.key_info"))
    }

    pub async fn get_file_status(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
    ) -> Result<ozone::OzoneFileStatusProto> {
        let mut om = self.request(ozone::Type::GetFileStatus);
        om.get_file_status_request = Some(ozone::GetFileStatusRequest {
            key_args: file_key_args(volume, bucket, key, None, &KeyReplication::default(), None),
        });
        let response = self.submit(om).await?;
        response
            .get_file_status_response
            .map(|r| r.status)
            .ok_or(Error::MissingField("get_file_status_response.status"))
    }

    pub async fn list_status(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        recursive: bool,
        start_key: &str,
        num_entries: u64,
    ) -> Result<Vec<ozone::OzoneFileStatusProto>> {
        let mut om = self.request(ozone::Type::ListStatus);
        om.list_status_request = Some(ozone::ListStatusRequest {
            key_args: file_key_args(volume, bucket, key, None, &KeyReplication::default(), None),
            recursive,
            start_key: start_key.to_string(),
            num_entries,
            allow_partial_prefix: Some(false),
        });
        let response = self.submit(om).await?;
        Ok(response
            .list_status_response
            .map(|r| r.statuses)
            .unwrap_or_default())
    }

    pub async fn create_directory(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        recursive: bool,
    ) -> Result<()> {
        let mut om = self.request(ozone::Type::CreateDirectory);
        om.create_directory_request = Some(ozone::CreateDirectoryRequest {
            key_args: file_key_args(
                volume,
                bucket,
                key,
                None,
                &KeyReplication::default(),
                Some(recursive),
            ),
        });
        self.submit(om).await?;
        Ok(())
    }

    pub async fn set_times(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        mtime: u64,
        atime: u64,
    ) -> Result<()> {
        let mut om = self.request(ozone::Type::SetTimes);
        om.set_times_request = Some(ozone::SetTimesRequest {
            key_args: file_key_args(volume, bucket, key, None, &KeyReplication::default(), None),
            mtime,
            atime,
        });
        self.submit(om).await?;
        Ok(())
    }

    pub async fn get_key_info(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
    ) -> Result<ozone::KeyInfo> {
        let mut om = self.request(ozone::Type::GetKeyInfo);
        om.get_key_info_request = Some(ozone::GetKeyInfoRequest {
            key_args: ozone::KeyArgs {
                volume_name: volume.to_string(),
                bucket_name: bucket.to_string(),
                key_name: key.to_string(),
                data_size: None,
                r#type: None,
                factor: None,
                key_locations: Vec::new(),
                is_multipart_key: None,
                multipart_upload_id: None,
                multipart_number: None,
                metadata: Vec::new(),
                acls: Vec::new(),
                modification_time: None,
                sort_datanodes: None,
                file_encryption_info: None,
                latest_version_location: Some(true),
                recursive: None,
                head_op: None,
                ec_replication_config: None,
                force_update_container_cache_from_scm: None,
                owner_name: None,
                tags: Vec::new(),
                expected_data_generation: None,
                expected_e_tag: None,
            },
            assume_s3_context: None,
        });
        let response = self.submit(om).await?;
        response
            .get_key_info_response
            .and_then(|r| r.key_info)
            .ok_or(Error::MissingField("get_key_info_response.key_info"))
    }

    pub async fn list_keys(
        &self,
        volume: &str,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<ozone::KeyInfo>> {
        let mut om = self.request(ozone::Type::ListKeys);
        om.list_keys_request = Some(ozone::ListKeysRequest {
            volume_name: volume.to_string(),
            bucket_name: bucket.to_string(),
            start_key: None,
            prefix: prefix.map(ToOwned::to_owned),
            count: Some(1024),
        });
        let response = self.submit(om).await?;
        if let Some(response) = response.list_keys_response {
            return Ok(response.key_info);
        }

        if let Some(response) = response.list_keys_light_response {
            return Ok(response
                .basic_key_info
                .into_iter()
                .map(|basic| ozone::KeyInfo {
                    volume_name: volume.to_string(),
                    bucket_name: bucket.to_string(),
                    key_name: basic.key_name.unwrap_or_default(),
                    data_size: basic.data_size.unwrap_or_default(),
                    r#type: basic.r#type.unwrap_or(hdds::ReplicationType::Ratis as i32),
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
                    is_file: basic.is_file,
                    owner_name: basic.owner_name,
                    tags: Vec::new(),
                    expected_data_generation: None,
                })
                .collect());
        }

        Ok(Vec::new())
    }

    pub async fn list_status_light(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        start_key: &str,
        num_entries: u64,
        allow_partial_prefix: bool,
    ) -> Result<Vec<ozone::OzoneFileStatusProtoLight>> {
        let mut om = self.request(ozone::Type::ListStatusLight);
        om.list_status_request = Some(ozone::ListStatusRequest {
            key_args: ozone::KeyArgs {
                volume_name: volume.to_string(),
                bucket_name: bucket.to_string(),
                key_name: key.to_string(),
                data_size: None,
                r#type: None,
                factor: None,
                key_locations: Vec::new(),
                is_multipart_key: None,
                multipart_upload_id: None,
                multipart_number: None,
                metadata: Vec::new(),
                acls: Vec::new(),
                modification_time: None,
                sort_datanodes: Some(false),
                file_encryption_info: None,
                latest_version_location: Some(true),
                recursive: None,
                head_op: None,
                ec_replication_config: None,
                force_update_container_cache_from_scm: None,
                owner_name: None,
                tags: Vec::new(),
                expected_data_generation: None,
                expected_e_tag: None,
            },
            recursive: false,
            start_key: start_key.to_string(),
            num_entries,
            allow_partial_prefix: Some(allow_partial_prefix),
        });
        let response = self.submit(om).await?;
        Ok(response
            .list_status_light_response
            .map(|r| r.statuses)
            .unwrap_or_default())
    }

    pub async fn delete_key(
        &self,
        volume: &str,
        bucket: &str,
        key: &str,
        recursive: bool,
    ) -> Result<()> {
        let mut om = self.request(ozone::Type::DeleteKey);
        om.delete_key_request = Some(ozone::DeleteKeyRequest {
            key_args: delete_key_args(volume, bucket, key, recursive),
        });
        self.submit(om).await?;
        Ok(())
    }

    pub async fn get_acl(&self, obj: ozone::OzoneObj) -> Result<Vec<ozone::OzoneAclInfo>> {
        let mut om = self.request(ozone::Type::GetAcl);
        om.get_acl_request = Some(ozone::GetAclRequest { obj });
        let response = self.submit(om).await?;
        Ok(response
            .get_acl_response
            .map(|response| response.acls)
            .unwrap_or_default())
    }

    pub async fn add_acl(&self, obj: ozone::OzoneObj, acl: ozone::OzoneAclInfo) -> Result<bool> {
        let mut om = self.request(ozone::Type::AddAcl);
        om.add_acl_request = Some(ozone::AddAclRequest {
            obj,
            acl,
            modification_time: None,
        });
        let response = self.submit(om).await?;
        Ok(response
            .add_acl_response
            .map(|response| response.response)
            .unwrap_or_default())
    }

    pub async fn remove_acl(&self, obj: ozone::OzoneObj, acl: ozone::OzoneAclInfo) -> Result<bool> {
        let mut om = self.request(ozone::Type::RemoveAcl);
        om.remove_acl_request = Some(ozone::RemoveAclRequest {
            obj,
            acl,
            modification_time: None,
        });
        let response = self.submit(om).await?;
        Ok(response
            .remove_acl_response
            .map(|response| response.response)
            .unwrap_or_default())
    }

    pub async fn set_acl(
        &self,
        obj: ozone::OzoneObj,
        acl: Vec<ozone::OzoneAclInfo>,
    ) -> Result<bool> {
        let mut om = self.request(ozone::Type::SetAcl);
        om.set_acl_request = Some(ozone::SetAclRequest {
            obj,
            acl,
            modification_time: None,
        });
        let response = self.submit(om).await?;
        Ok(response
            .set_acl_response
            .map(|response| response.response)
            .unwrap_or_default())
    }
}
