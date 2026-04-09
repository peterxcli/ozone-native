use crate::retry_window::RetryWindow;
use std::collections::BTreeMap;

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

    pub fn observe_write(&mut self, len: u64) {
        self.written_len += len;
    }

    pub fn next_flush_action(&self) -> Option<FlushAction> {
        if self.written_len.saturating_sub(self.flush_len) < self.flush_size {
            return None;
        }

        let flush_len = self.written_len;
        if self.piggyback_enabled {
            Some(FlushAction::PiggybackLastChunk { flush_len })
        } else {
            Some(FlushAction::StandalonePutBlock { flush_len })
        }
    }

    pub fn record_flush(&mut self, flush_len: u64, log_index: u64) {
        self.flush_len = self.flush_len.max(flush_len);
        self.pending_flushes
            .insert(flush_len, PendingFlush { log_index, acked: false });
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

#[allow(dead_code)]
pub struct BlockWriter {
    state: BlockWriterState,
    retry_window: RetryWindow,
}

#[cfg(test)]
mod tests {
    use super::{BlockWriterState, FlushAction};

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
}
