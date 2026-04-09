use crate::error::{Error, Result};
use crate::proto::hadoop::hdds;
use crate::proto::hadoop::hdds::datanode;
use crate::proto::ratis::common;
use crate::proto::ratis::common::raft_client_reply_proto::ExceptionDetails;
use crate::proto::ratis::common::raft_client_request_proto::Type as RequestType;
use crate::proto::ratis::grpc::raft_client_protocol_service_client::RaftClientProtocolServiceClient;
use crate::util::{
    datanode_ratis_client_address, datanode_uuid_string, encode_container_command_message,
    leader_node, normalize_endpoint, pipeline_uuid, uuid_to_bytes, MAX_GRPC_MESSAGE_SIZE,
};
use bytes::Bytes;
use prost::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::{sleep, Duration};
use tokio_stream::iter;
use tonic::transport::Channel;
use uuid::Uuid;

pub struct RatisClient {
    client_id: Uuid,
    next_call_id: AtomicU64,
    host_override: Option<String>,
}

impl Default for RatisClient {
    fn default() -> Self {
        Self {
            client_id: Uuid::new_v4(),
            next_call_id: AtomicU64::new(1),
            host_override: None,
        }
    }
}

impl RatisClient {
    pub fn new(host_override: Option<String>) -> Self {
        Self {
            host_override,
            ..Self::default()
        }
    }

    pub async fn write_container_command(
        &self,
        pipeline: &hdds::Pipeline,
        request: datanode::ContainerCommandRequestProto,
    ) -> Result<u64> {
        let reply = self
            .send_container_request(
                pipeline,
                RequestType::Write(common::WriteRequestTypeProto {
                    replication: common::ReplicationLevel::Majority as i32,
                }),
                Some(encode_container_command_message(&request)?),
            )
            .await?;

        let message = reply
            .message
            .ok_or(Error::MissingField("ratis write reply.message"))?;
        let response = datanode::ContainerCommandResponseProto::decode(message.content)?;
        Self::ensure_container_success(response)?;
        Ok(reply.log_index)
    }

    pub async fn watch(&self, pipeline: &hdds::Pipeline, log_index: u64) -> Result<()> {
        self.send_container_request(
            pipeline,
            RequestType::Watch(common::WatchRequestTypeProto {
                index: log_index,
                replication: common::ReplicationLevel::MajorityCommitted as i32,
            }),
            None,
        )
        .await?;
        Ok(())
    }

    async fn send_container_request(
        &self,
        pipeline: &hdds::Pipeline,
        request_type: RequestType,
        message: Option<Bytes>,
    ) -> Result<common::RaftClientReplyProto> {
        let group_id = pipeline_uuid(&pipeline.id)?;
        let initial_leader = leader_node(pipeline)
            .ok_or(Error::MissingField("pipeline leader"))?
            .clone();
        let mut leader = initial_leader.clone();
        let mut target = initial_leader;

        for _ in 0..3 {
            let call_id = self.next_call_id.fetch_add(1, Ordering::Relaxed);
            let request =
                self.build_request(group_id, &leader, call_id, request_type, message.clone())?;
            let reply = self.send_unordered(&target, request).await?;

            if reply
                .rpc_reply
                .as_ref()
                .map(|rpc| rpc.success)
                .unwrap_or(false)
            {
                return Ok(reply);
            }

            match reply.exception_details {
                Some(ExceptionDetails::NotLeaderException(not_leader)) => {
                    if let Some(suggested) = not_leader.suggested_leader {
                        let next_target = Self::peer_proto_to_node(&suggested);
                        leader = next_target.clone();
                        target = next_target;
                        continue;
                    }
                    if let Some(next_peer) = not_leader.peers_in_conf.first() {
                        let next_target = Self::peer_proto_to_node(next_peer);
                        leader = next_target.clone();
                        target = next_target;
                        continue;
                    }
                    return Err(Error::Ratis(
                        "leader redirection without suggested leader".to_string(),
                    ));
                }
                Some(ExceptionDetails::LeaderNotReadyException(_)) => {
                    sleep(Duration::from_millis(200)).await;
                    continue;
                }
                Some(ExceptionDetails::StateMachineException(err)) => {
                    return Err(Error::Ratis(format!(
                        "{}: {}",
                        err.exception_class_name, err.error_msg
                    )));
                }
                Some(ExceptionDetails::AlreadyClosedException(err)) => {
                    return Err(Error::Ratis(format!(
                        "{}: {}",
                        err.exception_class_name, err.error_msg
                    )));
                }
                Some(ExceptionDetails::NotReplicatedException(err)) => {
                    return Err(Error::Ratis(format!(
                        "not replicated to {:?} at log index {}",
                        common::ReplicationLevel::try_from(err.replication)
                            .unwrap_or(common::ReplicationLevel::Majority),
                        err.log_index
                    )));
                }
                Some(ExceptionDetails::DataStreamException(err))
                | Some(ExceptionDetails::LeaderSteppingDownException(err))
                | Some(ExceptionDetails::TransferLeadershipException(err))
                | Some(ExceptionDetails::ReadException(err))
                | Some(ExceptionDetails::ReadIndexException(err)) => {
                    return Err(Error::Ratis(format!(
                        "{}: {}",
                        err.class_name, err.error_message
                    )));
                }
                None => {
                    return Err(Error::Ratis(
                        "ratis reply failed without exception details".to_string(),
                    ));
                }
            }
        }

        Err(Error::Ratis("exhausted ratis retry attempts".to_string()))
    }

    fn build_request(
        &self,
        group_id: Uuid,
        leader: &hdds::DatanodeDetailsProto,
        call_id: u64,
        request_type: RequestType,
        message: Option<Bytes>,
    ) -> Result<common::RaftClientRequestProto> {
        let leader_id = datanode_uuid_string(leader)?;
        Ok(common::RaftClientRequestProto {
            rpc_request: Some(common::RaftRpcRequestProto {
                requestor_id: uuid_to_bytes(self.client_id),
                reply_id: leader_id.into_bytes(),
                raft_group_id: Some(common::RaftGroupIdProto {
                    id: uuid_to_bytes(group_id).into(),
                }),
                call_id,
                to_leader: true,
                span_context: None,
                replied_call_ids: Vec::new(),
                timeout_ms: 0,
                routing_table: None,
                sliding_window_entry: None,
            }),
            message: message.map(|content| common::ClientMessageEntryProto { content }),
            r#type: Some(request_type),
        })
    }

    async fn send_unordered(
        &self,
        target: &hdds::DatanodeDetailsProto,
        request: common::RaftClientRequestProto,
    ) -> Result<common::RaftClientReplyProto> {
        let address = datanode_ratis_client_address(target, self.host_override.as_deref())?;
        let channel = Channel::from_shared(normalize_endpoint(&address))
            .map_err(|e| Error::InvalidState(format!("invalid ratis endpoint: {e}")))?
            .connect()
            .await?;
        let mut client = RaftClientProtocolServiceClient::new(channel)
            .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE);
        let response = client.unordered(iter(vec![request])).await?;
        let mut stream = response.into_inner();
        stream
            .message()
            .await?
            .ok_or(Error::MissingField("ratis reply stream item"))
    }

    fn ensure_container_success(
        response: datanode::ContainerCommandResponseProto,
    ) -> Result<datanode::ContainerCommandResponseProto> {
        let result = datanode::Result::try_from(response.result)
            .unwrap_or(datanode::Result::ContainerInternalError);
        if result != datanode::Result::Success {
            return Err(Error::Datanode {
                result,
                message: response
                    .message
                    .unwrap_or_else(|| "datanode write failed without message".to_string()),
            });
        }
        Ok(response)
    }

    fn peer_proto_to_node(peer: &common::RaftPeerProto) -> hdds::DatanodeDetailsProto {
        let target = if !peer.client_address.is_empty() {
            peer.client_address.clone()
        } else {
            peer.address.clone()
        };
        let (host, port) = target
            .rsplit_once(':')
            .map(|(host, port)| (host.to_string(), port.parse::<u32>().unwrap_or(9858)))
            .unwrap_or_else(|| (target, 9858));
        let uuid = String::from_utf8_lossy(&peer.id).to_string();

        hdds::DatanodeDetailsProto {
            uuid: Some(uuid),
            ip_address: host.clone(),
            host_name: host,
            ports: vec![hdds::Port {
                name: "RATIS".to_string(),
                value: port,
            }],
            cert_serial_id: None,
            network_name: None,
            network_location: None,
            persisted_op_state: None,
            persisted_op_state_expiry: None,
            current_version: None,
            uuid128: None,
            level: None,
            id: None,
        }
    }
}
