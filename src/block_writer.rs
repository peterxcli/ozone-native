use crate::client::ClientConfig;
use crate::error::{Error, Result};
use crate::proto::hadoop::hdds;
use crate::proto::hadoop::hdds::datanode;
use crate::proto::hadoop::ozone;
use crate::proto::ratis::common;
use crate::proto::ratis::common::raft_client_request_proto::Type as RequestType;
use crate::ratis::RatisClient;
use crate::ratis_stream::{PendingReply, UnorderedRequestManager};
use crate::retry_window::{RetryChunk, RetryPlan, RetryWindow};
use crate::util::{
    block_id_to_datanode, datanode_uuid_string, encode_container_command_message, leader_node,
    no_checksum_data, pipeline_uuid, token_proto_to_url_string, CLIENT_VERSION,
};
use prost::Message;
use std::collections::BTreeMap;

const INCREMENTAL_CHUNK_LIST_KEY: &str = "INCREMENTAL_CHUNK_LIST";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FlushAction {
    StandalonePutBlock { flush_len: u64 },
    PiggybackLastChunk { flush_len: u64 },
}

#[derive(Clone, Debug)]
struct PendingFlush {
    log_index: u64,
    acked: bool,
}

#[derive(Default)]
pub struct BlockWriterState {
    written_len: u64,
    flush_len: u64,
    acked_len: u64,
    flush_size: u64,
    window_size: u64,
    piggyback_enabled: bool,
    pending_flushes: BTreeMap<u64, PendingFlush>,
}

impl BlockWriterState {
    pub fn new(_chunk_size: u64, flush_size: u64) -> Self {
        Self {
            flush_size,
            window_size: flush_size,
            ..Self::default()
        }
    }

    pub fn set_put_block_piggybacking(&mut self, enabled: bool) {
        self.piggyback_enabled = enabled;
    }

    pub fn set_window_size(&mut self, window_size: u64) {
        self.window_size = window_size;
    }

    pub fn observe_write(&mut self, len: u64) {
        self.written_len += len;
    }

    pub fn written_len(&self) -> u64 {
        self.written_len
    }

    pub fn should_wait_for_window(&self) -> bool {
        self.window_size > 0 && self.written_len.saturating_sub(self.acked_len) >= self.window_size
    }

    #[cfg(test)]
    pub fn next_flush_action(&self) -> Option<FlushAction> {
        self.flush_action_for(self.written_len, false)
    }

    pub fn flush_action_for(&self, candidate_flush_len: u64, force: bool) -> Option<FlushAction> {
        if !force && candidate_flush_len.saturating_sub(self.flush_len) < self.flush_size {
            return None;
        }

        if candidate_flush_len <= self.flush_len {
            return None;
        }

        if self.piggyback_enabled {
            Some(FlushAction::PiggybackLastChunk {
                flush_len: candidate_flush_len,
            })
        } else {
            Some(FlushAction::StandalonePutBlock {
                flush_len: candidate_flush_len,
            })
        }
    }

    pub fn record_flush(&mut self, flush_len: u64, log_index: u64) {
        self.flush_len = self.flush_len.max(flush_len);
        self.pending_flushes.insert(
            flush_len,
            PendingFlush {
                log_index,
                acked: false,
            },
        );
    }

    pub fn record_watch_success(&mut self, log_index: u64) {
        if let Some((_, pending)) = self
            .pending_flushes
            .iter_mut()
            .find(|(_, pending)| pending.log_index == log_index)
        {
            pending.acked = true;
        }

        loop {
            let Some((&flush_len, pending)) = self.pending_flushes.first_key_value() else {
                break;
            };
            if !pending.acked {
                break;
            }

            self.acked_len = flush_len;
            self.pending_flushes.remove(&flush_len);
        }
    }

    pub fn acked_len(&self) -> u64 {
        self.acked_len
    }
}

struct BlockTarget {
    location: ozone::KeyLocation,
    datanode_block_id: datanode::DatanodeBlockId,
    container_id: i64,
    datanode_uuid: String,
    pipeline_id: Option<String>,
    token: Option<String>,
    local_id: i64,
}

struct PendingOperation {
    receiver: PendingReply,
    flush: Option<FlushSubmission>,
}

#[derive(Clone, Copy)]
struct FlushSubmission {
    flush_len: u64,
    chunk_count: usize,
}

pub struct BlockWriter {
    ratis: RatisClient,
    pipeline: hdds::Pipeline,
    manager: Option<UnorderedRequestManager>,
    target: Option<BlockTarget>,
    config: ClientConfig,
    state: BlockWriterState,
    retry_window: RetryWindow,
    all_chunks: Vec<datanode::ChunkInfo>,
    last_acked_chunk_count: usize,
    chunk_index: usize,
}

impl BlockWriter {
    pub async fn new(
        ratis: &RatisClient,
        config: &ClientConfig,
        location: &ozone::KeyLocation,
    ) -> Result<Self> {
        let pipeline = location
            .pipeline
            .as_ref()
            .ok_or(Error::MissingField("key_location.pipeline"))?;
        let leader = leader_node(pipeline).ok_or(Error::MissingField("pipeline leader"))?;
        let manager = ratis.open_unordered_stream(pipeline).await?;
        let token = location
            .token
            .as_ref()
            .map(token_proto_to_url_string)
            .transpose()?;

        let mut state =
            BlockWriterState::new(config.chunk_size as u64, config.stream_flush_size as u64);
        state.set_window_size(config.stream_window_size as u64);
        state.set_put_block_piggybacking(config.enable_put_block_piggybacking);

        Ok(Self {
            ratis: ratis.clone(),
            pipeline: pipeline.clone(),
            manager: Some(manager),
            target: Some(BlockTarget {
                location: location.clone(),
                datanode_block_id: block_id_to_datanode(&location.block_id, None)?,
                container_id: location.block_id.container_block_id.container_id,
                datanode_uuid: datanode_uuid_string(leader)?,
                pipeline_id: Some(pipeline_uuid(&pipeline.id)?.to_string()),
                token,
                local_id: location.block_id.container_block_id.local_id,
            }),
            config: config.clone(),
            state,
            retry_window: RetryWindow::new(),
            all_chunks: Vec::new(),
            last_acked_chunk_count: 0,
            chunk_index: 0,
        })
    }

    #[cfg(test)]
    pub fn for_test(chunk_size: u64, flush_size: u64) -> Self {
        let mut state = BlockWriterState::new(chunk_size, flush_size);
        state.set_window_size(flush_size);
        Self {
            ratis: RatisClient::default(),
            pipeline: hdds::Pipeline::default(),
            manager: None,
            target: None,
            config: ClientConfig::default(),
            state,
            retry_window: RetryWindow::new(),
            all_chunks: Vec::new(),
            last_acked_chunk_count: 0,
            chunk_index: 0,
        }
    }

    #[cfg(test)]
    pub fn track_chunk_for_test(&mut self, start_offset: u64, end_offset: u64) {
        self.retry_window.track_write_chunk(RetryChunk::new(
            start_offset,
            end_offset,
            vec![0; (end_offset - start_offset) as usize],
        ));
    }

    #[cfg(test)]
    pub fn track_flush_for_test(&mut self, flush_len: u64) {
        self.retry_window.track_put_block(flush_len);
    }

    #[cfg(test)]
    pub fn acknowledge_for_test(&mut self, acked_len: u64) {
        self.retry_window.acknowledge_up_to(acked_len);
    }

    #[cfg(test)]
    pub fn retry_plan_for_test(&self) -> RetryPlan {
        self.retry_window.optimize_for_retry()
    }

    pub async fn write_all(mut self, data: &[u8]) -> Result<Vec<ozone::KeyLocation>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        let mut offset = 0usize;
        while offset < data.len() {
            let batch_limit = if self.config.stream_window_size == 0 {
                data.len()
            } else {
                (offset + self.config.stream_window_size).min(data.len())
            };
            let mut pending = Vec::new();
            let mut resend_unacked = false;

            while offset < batch_limit {
                let remaining = batch_limit - offset;
                let chunk_len = self.config.chunk_size.min(remaining);
                let next_offset = offset + chunk_len;
                let chunk_data = data[offset..next_offset].to_vec();
                let chunk_info = self.build_chunk_info(offset as u64, chunk_len as u64);
                self.state.observe_write(chunk_len as u64);
                self.retry_window.track_write_chunk(RetryChunk::new(
                    offset as u64,
                    next_offset as u64,
                    chunk_data.clone(),
                ));
                self.all_chunks.push(chunk_info.clone());

                let is_final_chunk = next_offset == data.len();
                let flush = self
                    .state
                    .flush_action_for(self.state.written_len(), is_final_chunk)
                    .map(|action| FlushSubmission {
                        flush_len: match action {
                            FlushAction::StandalonePutBlock { flush_len }
                            | FlushAction::PiggybackLastChunk { flush_len } => flush_len,
                        },
                        chunk_count: self.all_chunks.len(),
                    });
                offset = next_offset;

                if self.config.enable_put_block_piggybacking && flush.is_some() {
                    let flush = flush.expect("flush");
                    self.retry_window.track_put_block(flush.flush_len);
                    let put_block = self.build_put_block_request(
                        flush.chunk_count,
                        flush.flush_len,
                        is_final_chunk,
                    )?;
                    let receiver = match self
                        .send_write_chunk(chunk_info.clone(), chunk_data, Some(put_block))
                        .await
                    {
                        Ok(receiver) => receiver,
                        Err(_) => {
                            resend_unacked = true;
                            break;
                        }
                    };
                    pending.push(PendingOperation {
                        receiver,
                        flush: Some(flush),
                    });
                } else {
                    let receiver = match self
                        .send_write_chunk(chunk_info.clone(), chunk_data, None)
                        .await
                    {
                        Ok(receiver) => receiver,
                        Err(_) => {
                            resend_unacked = true;
                            break;
                        }
                    };
                    pending.push(PendingOperation {
                        receiver,
                        flush: None,
                    });
                    if let Some(flush) = flush {
                        self.retry_window.track_put_block(flush.flush_len);
                        let receiver = match self
                            .send_put_block(flush.chunk_count, flush.flush_len, is_final_chunk)
                            .await
                        {
                            Ok(receiver) => receiver,
                            Err(_) => {
                                resend_unacked = true;
                                break;
                            }
                        };
                        pending.push(PendingOperation {
                            receiver,
                            flush: Some(flush),
                        });
                    }
                }
            }

            if resend_unacked {
                self.retry_unacked_tail(data.len() as u64).await?;
                continue;
            }

            if let Err(_) = self.await_pending_operations(pending).await {
                self.retry_unacked_tail(data.len() as u64).await?;
                continue;
            }

            if self.state.should_wait_for_window() && self.state.acked_len() == 0 {
                self.retry_unacked_tail(data.len() as u64).await?;
            }
        }

        Ok(vec![self.committed_location(data.len() as u64)?])
    }

    fn build_chunk_info(&mut self, offset: u64, chunk_len: u64) -> datanode::ChunkInfo {
        let target = self.target.as_ref().expect("block writer target");
        let chunk_info = datanode::ChunkInfo {
            chunk_name: format!("{}_chunk_{}", target.local_id, self.chunk_index),
            offset,
            len: chunk_len,
            metadata: Vec::new(),
            checksum_data: no_checksum_data(),
            stripe_checksum: None,
        };
        self.chunk_index += 1;
        chunk_info
    }

    fn build_put_block_request(
        &self,
        chunk_count: usize,
        flush_len: u64,
        eof: bool,
    ) -> Result<datanode::PutBlockRequestProto> {
        let target = self.target.as_ref().ok_or_else(|| {
            Error::InvalidState("block writer target missing for putBlock".to_string())
        })?;

        let (metadata, chunks) = if self.config.enable_incremental_chunk_list {
            (
                vec![datanode::KeyValue {
                    key: INCREMENTAL_CHUNK_LIST_KEY.to_string(),
                    value: None,
                }],
                self.all_chunks[self.last_acked_chunk_count..chunk_count].to_vec(),
            )
        } else {
            (Vec::new(), self.all_chunks[..chunk_count].to_vec())
        };

        Ok(datanode::PutBlockRequestProto {
            block_data: datanode::BlockData {
                block_id: target.datanode_block_id,
                flags: None,
                metadata,
                chunks,
                size: Some(flush_len as i64),
            },
            eof: Some(eof),
        })
    }

    async fn send_write_chunk(
        &self,
        chunk_info: datanode::ChunkInfo,
        data: Vec<u8>,
        put_block: Option<datanode::PutBlockRequestProto>,
    ) -> Result<PendingReply> {
        let target = self.target.as_ref().ok_or_else(|| {
            Error::InvalidState("block writer target missing for writeChunk".to_string())
        })?;
        #[allow(deprecated)]
        let request = datanode::ContainerCommandRequestProto {
            cmd_type: datanode::Type::WriteChunk as i32,
            trace_id: None,
            container_id: target.container_id,
            datanode_uuid: target.datanode_uuid.clone(),
            pipeline_id: target.pipeline_id.clone(),
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
                block_id: target.datanode_block_id,
                chunk_data: Some(chunk_info),
                data: Some(data),
                block: put_block,
            }),
            delete_chunk: None,
            list_chunk: None,
            put_small_file: None,
            get_small_file: None,
            get_committed_block_length: None,
            encoded_token: target.token.clone(),
            version: Some(CLIENT_VERSION),
            finalize_block: None,
            echo: None,
            get_container_checksum_info: None,
            read_block: None,
        };

        self.manager()?
            .send_async(
                RequestType::Write(common::WriteRequestTypeProto {
                    replication: common::ReplicationLevel::Majority as i32,
                }),
                Some(encode_container_command_message(&request)?),
            )
            .await
    }

    async fn send_put_block(
        &self,
        chunk_count: usize,
        flush_len: u64,
        eof: bool,
    ) -> Result<PendingReply> {
        let target = self.target.as_ref().ok_or_else(|| {
            Error::InvalidState("block writer target missing for putBlock".to_string())
        })?;
        #[allow(deprecated)]
        let request = datanode::ContainerCommandRequestProto {
            cmd_type: datanode::Type::PutBlock as i32,
            trace_id: None,
            container_id: target.container_id,
            datanode_uuid: target.datanode_uuid.clone(),
            pipeline_id: target.pipeline_id.clone(),
            create_container: None,
            read_container: None,
            update_container: None,
            delete_container: None,
            list_container: None,
            close_container: None,
            put_block: Some(self.build_put_block_request(chunk_count, flush_len, eof)?),
            get_block: None,
            delete_block: None,
            list_block: None,
            read_chunk: None,
            write_chunk: None,
            delete_chunk: None,
            list_chunk: None,
            put_small_file: None,
            get_small_file: None,
            get_committed_block_length: None,
            encoded_token: target.token.clone(),
            version: Some(CLIENT_VERSION),
            finalize_block: None,
            echo: None,
            get_container_checksum_info: None,
            read_block: None,
        };

        self.manager()?
            .send_async(
                RequestType::Write(common::WriteRequestTypeProto {
                    replication: common::ReplicationLevel::Majority as i32,
                }),
                Some(encode_container_command_message(&request)?),
            )
            .await
    }

    async fn watch_for_commit(&self, log_index: u64) -> Result<()> {
        self.manager()?
            .send(
                RequestType::Watch(common::WatchRequestTypeProto {
                    index: log_index,
                    replication: common::ReplicationLevel::MajorityCommitted as i32,
                }),
                None,
            )
            .await?;
        Ok(())
    }

    async fn await_container_success(&self, receiver: PendingReply) -> Result<u64> {
        let reply = receiver
            .await
            .map_err(|_| Error::Ratis("ratis reply channel closed".to_string()))??;
        let response = datanode::ContainerCommandResponseProto::decode(reply.payload.clone())?;
        ensure_container_success(response)?;
        Ok(reply.log_index)
    }

    fn committed_location(&self, length: u64) -> Result<ozone::KeyLocation> {
        let target = self.target.as_ref().ok_or_else(|| {
            Error::InvalidState("block writer target missing for commit".to_string())
        })?;
        let mut location = target.location.clone();
        location.offset = 0;
        location.length = length;
        Ok(location)
    }

    fn manager(&self) -> Result<&UnorderedRequestManager> {
        self.manager
            .as_ref()
            .ok_or_else(|| Error::InvalidState("block writer manager missing".to_string()))
    }

    async fn await_pending_operations(&mut self, pending: Vec<PendingOperation>) -> Result<()> {
        for pending_op in pending {
            let log_index = self.await_container_success(pending_op.receiver).await?;
            if let Some(flush) = pending_op.flush {
                self.state.record_flush(flush.flush_len, log_index);
                if self.config.watch_for_commit {
                    self.watch_for_commit(log_index).await?;
                }
                self.state.record_watch_success(log_index);
                self.retry_window.acknowledge_up_to(flush.flush_len);
                self.last_acked_chunk_count = flush.chunk_count;
            }
        }
        Ok(())
    }

    async fn retry_unacked_tail(&mut self, total_len: u64) -> Result<()> {
        for _ in 0..=self.config.max_write_retries {
            let plan = self.retry_window.optimize_for_retry();
            if plan.chunks.is_empty() && plan.put_block_offset.is_none() {
                return Ok(());
            }

            self.manager = Some(self.ratis.open_unordered_stream(&self.pipeline).await?);
            match self.replay_plan(plan, total_len).await {
                Ok(()) => return Ok(()),
                Err(_) => continue,
            }
        }

        Err(Error::Ratis(
            "exhausted retry-window resend attempts".to_string(),
        ))
    }

    async fn replay_plan(&mut self, plan: RetryPlan, total_len: u64) -> Result<()> {
        let flush = if let Some(flush_len) = plan.put_block_offset {
            Some(FlushSubmission {
                flush_len,
                chunk_count: self.chunk_count_for_offset(flush_len)?,
            })
        } else {
            None
        };
        let mut pending = Vec::new();

        for (index, chunk) in plan.chunks.iter().enumerate() {
            let piggyback_flush = flush.filter(|flush| {
                self.config.enable_put_block_piggybacking
                    && flush.flush_len == chunk.end_offset
                    && index + 1 == plan.chunks.len()
            });
            let receiver = self
                .send_write_chunk(
                    self.chunk_info_for_retry(chunk)?,
                    chunk.data.clone(),
                    piggyback_flush
                        .map(|flush| {
                            self.build_put_block_request(
                                flush.chunk_count,
                                flush.flush_len,
                                flush.flush_len == total_len,
                            )
                        })
                        .transpose()?,
                )
                .await?;
            pending.push(PendingOperation {
                receiver,
                flush: piggyback_flush,
            });
        }

        if let Some(flush) = flush.filter(|flush| {
            !(self.config.enable_put_block_piggybacking
                && plan
                    .chunks
                    .last()
                    .is_some_and(|chunk| chunk.end_offset == flush.flush_len))
        }) {
            let receiver = self
                .send_put_block(
                    flush.chunk_count,
                    flush.flush_len,
                    flush.flush_len == total_len,
                )
                .await?;
            pending.push(PendingOperation {
                receiver,
                flush: Some(flush),
            });
        }

        self.await_pending_operations(pending).await
    }

    fn chunk_count_for_offset(&self, flush_len: u64) -> Result<usize> {
        let chunk_count = self
            .all_chunks
            .iter()
            .take_while(|chunk| chunk.offset + chunk.len <= flush_len)
            .count();
        if chunk_count == 0 && flush_len > 0 {
            return Err(Error::InvalidState(format!(
                "no chunks recorded for flush offset {flush_len}"
            )));
        }
        Ok(chunk_count)
    }

    fn chunk_info_for_retry(&self, retry_chunk: &RetryChunk) -> Result<datanode::ChunkInfo> {
        self.all_chunks
            .iter()
            .find(|chunk| {
                chunk.offset == retry_chunk.start_offset
                    && chunk.len == retry_chunk.end_offset - retry_chunk.start_offset
            })
            .cloned()
            .ok_or_else(|| {
                Error::InvalidState(format!(
                    "missing chunk metadata for retry range {}..{}",
                    retry_chunk.start_offset, retry_chunk.end_offset
                ))
            })
    }
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

#[cfg(test)]
mod tests {
    use super::{BlockWriter, BlockWriterState, FlushAction};

    #[test]
    fn flush_boundary_is_created_when_written_minus_flush_reaches_threshold() {
        let mut state = BlockWriterState::new(4, 8);
        state.observe_write(4);
        assert_eq!(state.next_flush_action(), None);

        state.observe_write(4);
        assert_eq!(
            state.next_flush_action(),
            Some(FlushAction::StandalonePutBlock { flush_len: 8 })
        );
    }

    #[test]
    fn contiguous_ack_frontier_does_not_jump_over_gaps() {
        let mut state = BlockWriterState::new(4, 8);
        state.record_flush(8, 101);
        state.record_flush(16, 102);

        state.record_watch_success(102);
        assert_eq!(state.acked_len(), 0);

        state.record_watch_success(101);
        assert_eq!(state.acked_len(), 16);
    }

    #[test]
    fn piggyback_is_used_for_last_chunk_when_enabled() {
        let mut state = BlockWriterState::new(4, 8);
        state.set_put_block_piggybacking(true);
        state.observe_write(8);

        assert_eq!(
            state.next_flush_action(),
            Some(FlushAction::PiggybackLastChunk { flush_len: 8 })
        );
    }

    #[test]
    fn retry_plan_replays_only_unacked_tail_and_one_terminal_put_block() {
        let mut writer = BlockWriter::for_test(4, 8);
        writer.track_chunk_for_test(0, 4);
        writer.track_flush_for_test(4);
        writer.track_chunk_for_test(4, 8);
        writer.track_flush_for_test(8);
        writer.acknowledge_for_test(4);

        let plan = writer.retry_plan_for_test();

        assert_eq!(plan.chunks.len(), 1);
        assert_eq!(plan.chunks[0].start_offset, 4);
        assert_eq!(plan.put_block_offset, Some(8));
    }
}
