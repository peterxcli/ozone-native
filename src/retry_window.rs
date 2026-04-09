#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryChunk {
    pub start_offset: u64,
    pub end_offset: u64,
    pub data: Vec<u8>,
}

impl RetryChunk {
    pub fn new(start_offset: u64, end_offset: u64, data: Vec<u8>) -> Self {
        Self {
            start_offset,
            end_offset,
            data,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RetryPlan {
    pub chunks: Vec<RetryChunk>,
    pub put_block_offset: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct RetryWindow {
    chunks: Vec<RetryChunk>,
    latest_put_block_offset: Option<u64>,
    acknowledged_offset: u64,
}

impl RetryWindow {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn track_write_chunk(&mut self, chunk: RetryChunk) {
        self.chunks.push(chunk);
    }

    pub fn track_put_block(&mut self, offset: u64) {
        if self
            .latest_put_block_offset
            .map_or(true, |current| offset > current)
        {
            self.latest_put_block_offset = Some(offset);
        }
    }

    pub fn acknowledge_up_to(&mut self, offset: u64) {
        self.acknowledged_offset = self.acknowledged_offset.max(offset);
        self.chunks
            .retain(|chunk| chunk.end_offset > self.acknowledged_offset);
        if self
            .latest_put_block_offset
            .is_some_and(|flush| flush <= self.acknowledged_offset)
        {
            self.latest_put_block_offset = None;
        }
    }

    pub fn optimize_for_retry(&self) -> RetryPlan {
        RetryPlan {
            chunks: self.chunks.clone(),
            put_block_offset: self.latest_put_block_offset,
        }
    }

    #[cfg(test)]
    pub fn acknowledged_offset(&self) -> u64 {
        self.acknowledged_offset
    }
}

#[cfg(test)]
mod tests {
    use super::{RetryChunk, RetryWindow};

    fn chunk(start: u64, end: u64) -> RetryChunk {
        RetryChunk::new(start, end, vec![0; (end - start) as usize])
    }

    #[test]
    fn optimize_for_retry_discards_acknowledged_chunks_and_keeps_latest_put_block() {
        let mut window = RetryWindow::new();
        window.track_write_chunk(chunk(0, 4));
        window.track_put_block(4);
        window.track_write_chunk(chunk(4, 8));
        window.track_put_block(8);
        window.acknowledge_up_to(4);

        let plan = window.optimize_for_retry();

        assert_eq!(plan.chunks.len(), 1);
        assert_eq!(plan.chunks[0].start_offset, 4);
        assert_eq!(plan.put_block_offset, Some(8));
    }

    #[test]
    fn acknowledge_up_to_clears_everything_when_window_fully_acked() {
        let mut window = RetryWindow::new();
        window.track_write_chunk(chunk(0, 4));
        window.track_put_block(4);
        window.acknowledge_up_to(4);

        let plan = window.optimize_for_retry();

        assert!(plan.chunks.is_empty());
        assert_eq!(plan.put_block_offset, None);
        assert_eq!(window.acknowledged_offset(), 4);
    }
}
