use crate::error::{Error, Result};
use crate::proto::hadoop::hdds;
use crate::proto::ratis::common;
use crate::proto::ratis::common::raft_client_request_proto::Type as RequestType;
use crate::proto::ratis::grpc::raft_client_protocol_service_client::RaftClientProtocolServiceClient;
use crate::util::{
    datanode_ratis_client_address, datanode_uuid_string, leader_node, normalize_endpoint,
    pipeline_uuid, uuid_to_bytes, MAX_GRPC_MESSAGE_SIZE,
};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::Request;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestStreamState {
    Open,
    Closed,
}

#[derive(Debug)]
pub struct StreamReply {
    pub log_index: u64,
    pub payload: Bytes,
    pub proto: common::RaftClientReplyProto,
}

impl StreamReply {
    fn from_proto(proto: common::RaftClientReplyProto) -> Result<(u64, Self)> {
        let call_id = proto
            .rpc_reply
            .as_ref()
            .ok_or(Error::MissingField("ratis reply.rpc_reply"))?
            .call_id;
        let payload = proto
            .message
            .as_ref()
            .map(|message| Bytes::copy_from_slice(&message.content))
            .unwrap_or_default();

        Ok((
            call_id,
            Self {
                log_index: proto.log_index,
                payload,
                proto,
            },
        ))
    }
}

#[derive(Default)]
pub struct PendingReplies {
    waiters: HashMap<u64, oneshot::Sender<Result<StreamReply>>>,
}

impl PendingReplies {
    pub fn insert(&mut self, call_id: u64) -> oneshot::Receiver<Result<StreamReply>> {
        let (tx, rx) = oneshot::channel();
        self.waiters.insert(call_id, tx);
        rx
    }

    pub fn complete_ok(&mut self, call_id: u64, log_index: u64) {
        if let Some(tx) = self.waiters.remove(&call_id) {
            let reply = StreamReply {
                log_index,
                payload: Bytes::new(),
                proto: common::RaftClientReplyProto {
                    rpc_reply: Some(common::RaftRpcReplyProto {
                        requestor_id: Vec::new(),
                        reply_id: Vec::new(),
                        raft_group_id: None,
                        call_id,
                        success: true,
                    }),
                    message: None,
                    log_index,
                    commit_infos: Vec::new(),
                    exception_details: None,
                },
            };
            let _ = tx.send(Ok(reply));
        }
    }

    pub fn complete(&mut self, call_id: u64, reply: Result<StreamReply>) {
        if let Some(tx) = self.waiters.remove(&call_id) {
            let _ = tx.send(reply);
        }
    }

    pub fn fail_all(&mut self, message: &str) {
        for (_, tx) in self.waiters.drain() {
            let _ = tx.send(Err(Error::Ratis(message.to_string())));
        }
    }
}

pub struct UnorderedRequestManager {
    request_tx: mpsc::Sender<common::RaftClientRequestProto>,
    pending: Arc<Mutex<PendingReplies>>,
    state: Arc<Mutex<RequestStreamState>>,
    client_id: Uuid,
    next_call_id: Arc<AtomicU64>,
    group_id: Uuid,
    leader_id: String,
}

impl UnorderedRequestManager {
    pub async fn connect(
        client_id: Uuid,
        next_call_id: Arc<AtomicU64>,
        pipeline: &hdds::Pipeline,
        host_override: Option<String>,
    ) -> Result<Self> {
        let leader = leader_node(pipeline).ok_or(Error::MissingField("pipeline leader"))?;
        let address = datanode_ratis_client_address(leader, host_override.as_deref())?;
        let channel = Channel::from_shared(normalize_endpoint(&address))
            .map_err(|e| Error::InvalidState(format!("invalid ratis endpoint: {e}")))?
            .connect()
            .await?;
        let mut client = RaftClientProtocolServiceClient::new(channel)
            .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE);

        let (request_tx, request_rx) = mpsc::channel(64);
        let response = client
            .unordered(Request::new(ReceiverStream::new(request_rx)))
            .await?;
        let mut responses = response.into_inner();

        let pending = Arc::new(Mutex::new(PendingReplies::default()));
        let state = Arc::new(Mutex::new(RequestStreamState::Open));

        let task_pending = Arc::clone(&pending);
        let task_state = Arc::clone(&state);
        tokio::spawn(async move {
            loop {
                match responses.message().await {
                    Ok(Some(reply_proto)) => {
                        let result = StreamReply::from_proto(reply_proto);
                        match result {
                            Ok((call_id, reply)) => {
                                task_pending.lock().await.complete(call_id, Ok(reply));
                            }
                            Err(err) => {
                                task_pending
                                    .lock()
                                    .await
                                    .fail_all(&format!("invalid ratis reply: {err}"));
                                *task_state.lock().await = RequestStreamState::Closed;
                                break;
                            }
                        }
                    }
                    Ok(None) => {
                        task_pending.lock().await.fail_all("ratis stream closed");
                        *task_state.lock().await = RequestStreamState::Closed;
                        break;
                    }
                    Err(status) => {
                        task_pending
                            .lock()
                            .await
                            .fail_all(&format!("ratis stream error: {status}"));
                        *task_state.lock().await = RequestStreamState::Closed;
                        break;
                    }
                }
            }
        });

        Ok(Self {
            request_tx,
            pending,
            state,
            client_id,
            next_call_id,
            group_id: pipeline_uuid(&pipeline.id)?,
            leader_id: datanode_uuid_string(leader)?,
        })
    }

    pub async fn send(
        &self,
        request_type: RequestType,
        message: Option<Bytes>,
    ) -> Result<StreamReply> {
        if *self.state.lock().await == RequestStreamState::Closed {
            return Err(Error::Ratis("ratis stream closed".to_string()));
        }

        let call_id = self.next_call_id.fetch_add(1, Ordering::Relaxed);
        let request = self.build_request(call_id, request_type, message);
        let receiver = self.pending.lock().await.insert(call_id);
        self.request_tx
            .send(request)
            .await
            .map_err(|_| Error::Ratis("failed to send request on ratis stream".to_string()))?;

        receiver
            .await
            .map_err(|_| Error::Ratis("ratis reply channel closed".to_string()))?
    }

    fn build_request(
        &self,
        call_id: u64,
        request_type: RequestType,
        message: Option<Bytes>,
    ) -> common::RaftClientRequestProto {
        common::RaftClientRequestProto {
            rpc_request: Some(common::RaftRpcRequestProto {
                requestor_id: uuid_to_bytes(self.client_id),
                reply_id: self.leader_id.clone().into_bytes(),
                raft_group_id: Some(common::RaftGroupIdProto {
                    id: uuid_to_bytes(self.group_id).into(),
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PendingReplies, RequestStreamState};

    #[tokio::test]
    async fn complete_reply_routes_response_by_call_id() {
        let _state = RequestStreamState::Open;
        let mut pending = PendingReplies::default();
        let rx = pending.insert(7);

        pending.complete_ok(7, 11);

        let reply = rx.await.expect("reply").expect("ok reply");
        assert_eq!(reply.log_index, 11);
    }

    #[tokio::test]
    async fn fail_all_notifies_every_waiter_when_stream_breaks() {
        let mut pending = PendingReplies::default();
        let first = pending.insert(1);
        let second = pending.insert(2);

        pending.fail_all("stream closed");

        assert!(first
            .await
            .expect("first receiver")
            .expect_err("first error")
            .to_string()
            .contains("stream closed"));
        assert!(second
            .await
            .expect("second receiver")
            .expect_err("second error")
            .to_string()
            .contains("stream closed"));
    }
}
