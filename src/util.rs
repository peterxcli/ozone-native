use crate::error::{Error, Result};
use crate::proto::hadoop::hdds::datanode;
use crate::proto::hadoop::{common, hdds, ozone};
use bytes::{BufMut, Bytes, BytesMut};
use prost::Message;
use uuid::Uuid;

pub const CLIENT_VERSION: u32 = 3;
pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;
pub const DEFAULT_STREAM_FLUSH_SIZE: usize = 16 * 1024 * 1024;
pub const DEFAULT_STREAM_WINDOW_SIZE: usize = 32 * 1024 * 1024;
pub const DEFAULT_READ_RESPONSE_SIZE: u32 = 1024 * 1024;
pub const DEFAULT_MAX_WRITE_RETRIES: usize = 5;
pub const MAX_GRPC_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

pub fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

pub fn new_trace_id() -> String {
    Uuid::new_v4().to_string()
}

pub fn uuid_to_bytes(uuid: Uuid) -> Vec<u8> {
    uuid.as_u128().to_be_bytes().to_vec()
}

pub fn hdds_uuid_to_uuid(uuid: &hdds::Uuid) -> Uuid {
    let msb = uuid.most_sig_bits as u64;
    let lsb = uuid.least_sig_bits as u64;
    Uuid::from_u128(((msb as u128) << 64) | (lsb as u128))
}

pub fn pipeline_uuid(pipeline_id: &hdds::PipelineId) -> Result<Uuid> {
    if let Some(uuid) = &pipeline_id.uuid128 {
        return Ok(hdds_uuid_to_uuid(uuid));
    }

    if let Some(id) = &pipeline_id.id {
        return Uuid::parse_str(id)
            .map_err(|e| Error::InvalidState(format!("invalid pipeline id {id}: {e}")));
    }

    Err(Error::MissingField("pipeline.id"))
}

pub fn datanode_uuid_string(node: &hdds::DatanodeDetailsProto) -> Result<String> {
    if let Some(uuid) = &node.uuid {
        return Ok(uuid.clone());
    }

    if let Some(uuid) = &node.uuid128 {
        return Ok(hdds_uuid_to_uuid(uuid).to_string());
    }

    if let Some(id) = &node.id {
        return Ok(hdds_uuid_to_uuid(&id.uuid).to_string());
    }

    Err(Error::MissingField("datanode.uuid"))
}

pub fn datanode_host<'a>(
    node: &'a hdds::DatanodeDetailsProto,
    host_override: Option<&'a str>,
) -> &'a str {
    if let Some(host) = host_override {
        host
    } else if !node.host_name.is_empty() {
        &node.host_name
    } else {
        &node.ip_address
    }
}

pub fn datanode_port(node: &hdds::DatanodeDetailsProto, names: &[&str]) -> Option<u32> {
    node.ports.iter().find_map(|port| {
        names
            .iter()
            .any(|name| port.name.eq_ignore_ascii_case(name))
            .then_some(port.value)
    })
}

pub fn datanode_standalone_address(
    node: &hdds::DatanodeDetailsProto,
    host_override: Option<&str>,
) -> Result<String> {
    let port = datanode_port(node, &["STANDALONE", "CLIENT_RPC"])
        .ok_or_else(|| Error::MissingField("datanode standalone port"))?;
    Ok(format!("{}:{port}", datanode_host(node, host_override)))
}

pub fn datanode_ratis_client_address(
    node: &hdds::DatanodeDetailsProto,
    host_override: Option<&str>,
) -> Result<String> {
    let port = datanode_port(node, &["RATIS", "RATIS_SERVER"])
        .ok_or_else(|| Error::MissingField("datanode ratis client port"))?;
    Ok(format!("{}:{port}", datanode_host(node, host_override)))
}

pub fn find_pipeline_member<'a>(
    pipeline: &'a hdds::Pipeline,
    uuid: &str,
) -> Option<&'a hdds::DatanodeDetailsProto> {
    pipeline
        .members
        .iter()
        .find(|member| datanode_uuid_string(member).ok().as_deref() == Some(uuid))
}

pub fn leader_node(pipeline: &hdds::Pipeline) -> Option<&hdds::DatanodeDetailsProto> {
    if let Some(id) = &pipeline.leader_datanode_id {
        let leader = hdds_uuid_to_uuid(&id.uuid).to_string();
        if let Some(member) = find_pipeline_member(pipeline, &leader) {
            return Some(member);
        }
    }

    if let Some(id) = &pipeline.leader_id {
        if let Some(member) = find_pipeline_member(pipeline, id) {
            return Some(member);
        }
    }

    if let Some(id) = &pipeline.leader_id128 {
        let leader = hdds_uuid_to_uuid(id).to_string();
        if let Some(member) = find_pipeline_member(pipeline, &leader) {
            return Some(member);
        }
    }

    pipeline.members.first()
}

pub fn latest_key_locations(key_info: &ozone::KeyInfo) -> Vec<ozone::KeyLocation> {
    key_info
        .key_location_list
        .last()
        .map(|locations| locations.key_locations.clone())
        .unwrap_or_default()
}

pub fn key_replication(
    key_info: &ozone::KeyInfo,
) -> (Option<i32>, Option<i32>, Option<hdds::EcReplicationConfig>) {
    (
        Some(key_info.r#type),
        key_info.factor,
        key_info.ec_replication_config.clone(),
    )
}

pub fn block_id_to_datanode(
    block_id: &hdds::BlockId,
    replica_index: Option<u32>,
) -> Result<datanode::DatanodeBlockId> {
    let container_block_id = &block_id.container_block_id;
    let replica_index = replica_index.map(|index| index as i32);

    Ok(datanode::DatanodeBlockId {
        container_id: container_block_id.container_id,
        local_id: container_block_id.local_id,
        block_commit_sequence_id: block_id.block_commit_sequence_id,
        replica_index,
    })
}

pub fn no_checksum_data() -> datanode::ChecksumData {
    datanode::ChecksumData {
        r#type: datanode::ChecksumType::None as i32,
        bytes_per_checksum: 0,
        checksums: Vec::new(),
    }
}

pub fn encode_container_command_message(
    request: &datanode::ContainerCommandRequestProto,
) -> Result<Bytes> {
    let mut header = request.clone();
    if header.version.is_none() {
        header.version = Some(CLIENT_VERSION);
    }

    let data = if let Some(write_chunk) = &header.write_chunk {
        let body = write_chunk.data.clone().unwrap_or_default();
        if let Some(write_chunk) = &mut header.write_chunk {
            write_chunk.data = None;
        }
        body
    } else if let Some(put_small_file) = &header.put_small_file {
        let body = put_small_file.data.clone();
        if let Some(put_small_file) = &mut header.put_small_file {
            put_small_file.data.clear();
        }
        body
    } else {
        Vec::new()
    };

    let header_bytes = header.encode_to_vec();
    let mut content = BytesMut::with_capacity(4 + header_bytes.len() + data.len());
    content.put_u32(header_bytes.len() as u32);
    content.extend_from_slice(&header_bytes);
    content.extend_from_slice(&data);
    Ok(content.freeze())
}

pub fn token_proto_to_url_string(_token: &common::TokenProto) -> Result<String> {
    Err(Error::Unsupported(
        "secure block-token encoding is not implemented yet".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::hadoop::{hdds, hdds::datanode, ozone};
    use prost::Message;

    #[test]
    #[allow(deprecated)]
    fn encodes_write_chunk_request_like_container_command_request_message() {
        let request = datanode::ContainerCommandRequestProto {
            cmd_type: datanode::Type::WriteChunk as i32,
            trace_id: Some("trace-1".to_string()),
            container_id: 99,
            datanode_uuid: "dn-1".to_string(),
            pipeline_id: Some("pipeline-1".to_string()),
            create_container: None,
            read_container: None,
            update_container: None,
            delete_container: None,
            list_container: None,
            close_container: None,
            put_block: None,
            get_block: None,
            delete_block: None,
            list_block: None,
            read_chunk: None,
            write_chunk: Some(datanode::WriteChunkRequestProto {
                block_id: datanode::DatanodeBlockId {
                    container_id: 99,
                    local_id: 7,
                    block_commit_sequence_id: Some(0),
                    replica_index: None,
                },
                chunk_data: Some(datanode::ChunkInfo {
                    chunk_name: "7_chunk_0".to_string(),
                    offset: 0,
                    len: 3,
                    metadata: Vec::new(),
                    checksum_data: no_checksum_data(),
                    stripe_checksum: None,
                }),
                data: Some(b"abc".to_vec()),
                block: None,
            }),
            delete_chunk: None,
            list_chunk: None,
            put_small_file: None,
            get_small_file: None,
            get_committed_block_length: None,
            encoded_token: None,
            version: Some(CLIENT_VERSION),
            finalize_block: None,
            echo: None,
            get_container_checksum_info: None,
            read_block: None,
        };

        let encoded = encode_container_command_message(&request).expect("encode");
        let header_len =
            u32::from_be_bytes(encoded[..4].try_into().expect("header length")) as usize;
        let header = datanode::ContainerCommandRequestProto::decode(&encoded[4..4 + header_len])
            .expect("decode");

        assert_eq!(header.cmd_type, datanode::Type::WriteChunk as i32);
        assert!(header.write_chunk.is_some());
        assert!(header
            .write_chunk
            .as_ref()
            .expect("write chunk")
            .data
            .is_none());
        assert_eq!(&encoded[4 + header_len..], b"abc");
    }

    #[test]
    fn latest_key_locations_uses_latest_version() {
        let first = ozone::KeyLocation {
            block_id: hdds::BlockId {
                container_block_id: hdds::ContainerBlockId {
                    container_id: 1,
                    local_id: 1,
                },
                block_commit_sequence_id: Some(0),
            },
            offset: 0,
            length: 10,
            create_version: Some(1),
            token: None,
            pipeline: None,
            part_number: None,
        };
        let second = ozone::KeyLocation {
            block_id: hdds::BlockId {
                container_block_id: hdds::ContainerBlockId {
                    container_id: 2,
                    local_id: 2,
                },
                block_commit_sequence_id: Some(0),
            },
            offset: 0,
            length: 20,
            create_version: Some(2),
            token: None,
            pipeline: None,
            part_number: None,
        };

        let key_info = ozone::KeyInfo {
            volume_name: "vol".to_string(),
            bucket_name: "bucket".to_string(),
            key_name: "key".to_string(),
            data_size: 20,
            r#type: hdds::ReplicationType::Ratis as i32,
            factor: Some(hdds::ReplicationFactor::One as i32),
            key_location_list: vec![
                ozone::KeyLocationList {
                    version: Some(1),
                    key_locations: vec![first],
                    file_encryption_info: None,
                    is_multipart_key: Some(false),
                },
                ozone::KeyLocationList {
                    version: Some(2),
                    key_locations: vec![second.clone()],
                    file_encryption_info: None,
                    is_multipart_key: Some(false),
                },
            ],
            creation_time: 0,
            modification_time: 0,
            latest_version: Some(2),
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
            expected_e_tag: None,
        };

        assert_eq!(latest_key_locations(&key_info), vec![second]);
    }

    #[test]
    fn leader_node_prefers_explicit_leader() {
        let leader_uuid = Uuid::new_v4();
        let follower_uuid = Uuid::new_v4();

        let leader = hdds::DatanodeDetailsProto {
            uuid: Some(leader_uuid.to_string()),
            ip_address: "127.0.0.1".to_string(),
            host_name: "leader".to_string(),
            ports: Vec::new(),
            cert_serial_id: None,
            network_name: None,
            network_location: None,
            persisted_op_state: None,
            persisted_op_state_expiry: None,
            current_version: None,
            uuid128: None,
            level: None,
            id: None,
        };
        let follower = hdds::DatanodeDetailsProto {
            uuid: Some(follower_uuid.to_string()),
            ip_address: "127.0.0.2".to_string(),
            host_name: "follower".to_string(),
            ports: Vec::new(),
            cert_serial_id: None,
            network_name: None,
            network_location: None,
            persisted_op_state: None,
            persisted_op_state_expiry: None,
            current_version: None,
            uuid128: None,
            level: None,
            id: None,
        };

        let pipeline = hdds::Pipeline {
            members: vec![follower, leader.clone()],
            state: Some(hdds::PipelineState::PipelineOpen as i32),
            r#type: Some(hdds::ReplicationType::Ratis as i32),
            factor: Some(hdds::ReplicationFactor::One as i32),
            id: hdds::PipelineId {
                id: Some(Uuid::new_v4().to_string()),
                uuid128: None,
            },
            leader_id: Some(leader_uuid.to_string()),
            member_orders: Vec::new(),
            creation_time_stamp: None,
            suggested_leader_id: None,
            member_replica_indexes: Vec::new(),
            ec_replication_config: None,
            leader_id128: None,
            leader_datanode_id: None,
            suggested_leader_datanode_id: None,
        };

        assert_eq!(
            datanode_uuid_string(leader_node(&pipeline).expect("leader")).expect("uuid"),
            leader_uuid.to_string()
        );
    }
}
