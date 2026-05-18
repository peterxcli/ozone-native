use crate::block_writer::BlockWriter;
use crate::error::{Error, Result};
use crate::proto::hadoop::ozone;

#[derive(Clone, Copy, Debug)]
struct ActiveBlockProgress {
    capacity: Option<u64>,
    written: u64,
}

impl ActiveBlockProgress {
    fn bounded(capacity: u64) -> Self {
        Self {
            capacity: Some(capacity),
            written: 0,
        }
    }

    fn from_location_len(len: u64, fallback_capacity: u64) -> Self {
        if len > 0 {
            Self::bounded(len)
        } else {
            Self::bounded(fallback_capacity.max(1))
        }
    }

    fn next_write_len(&self, requested: usize) -> usize {
        self.remaining()
            .map(|remaining| requested.min(remaining as usize))
            .unwrap_or(requested)
    }

    fn observe_write(&mut self, len: u64) {
        self.written += len;
    }

    fn is_full(&self) -> bool {
        self.remaining() == Some(0)
    }

    fn remaining(&self) -> Option<u64> {
        self.capacity
            .map(|capacity| capacity.saturating_sub(self.written))
    }
}

pub(crate) struct WritableBlock {
    location: ozone::KeyLocation,
    writer: Option<BlockWriter>,
    progress: ActiveBlockProgress,
    closed: bool,
}

impl WritableBlock {
    fn new(location: ozone::KeyLocation, fallback_capacity: u64) -> Self {
        Self {
            progress: ActiveBlockProgress::from_location_len(location.length, fallback_capacity),
            location,
            writer: None,
            closed: false,
        }
    }

    pub(crate) fn next_write_len(&self, requested: usize) -> usize {
        self.progress.next_write_len(requested)
    }

    fn observe_write(&mut self, len: usize) {
        self.progress.observe_write(len as u64);
    }

    pub(crate) fn attach_writer(&mut self, writer: BlockWriter) -> Result<()> {
        if self.closed {
            return Err(Error::InvalidState(
                "cannot attach writer to a closed block".to_string(),
            ));
        }
        if self.writer.is_some() {
            return Err(Error::InvalidState(
                "active block writer already attached".to_string(),
            ));
        }
        self.writer = Some(writer);
        Ok(())
    }

    pub(crate) async fn write(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        if self.closed {
            return Err(Error::InvalidState(
                "cannot write to a closed block".to_string(),
            ));
        }

        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| Error::InvalidState("active block writer missing".to_string()))?;
        writer.write(data).await?;
        self.observe_write(data.len());
        Ok(())
    }

    pub(crate) fn is_full(&self) -> bool {
        self.progress.is_full()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.progress.written == 0
    }

    pub(crate) fn needs_writer(&self) -> bool {
        !self.closed && self.writer.is_none()
    }

    pub(crate) async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        if self.is_empty() {
            self.closed = true;
            return Ok(());
        }

        let writer = self
            .writer
            .take()
            .ok_or_else(|| Error::InvalidState("active block writer missing".to_string()))?;
        let location = writer.close().await?;
        self.update_committed_location(location);
        Ok(())
    }

    fn update_committed_location(&mut self, mut location: ozone::KeyLocation) {
        location.length = self.progress.written;
        self.location = location;
        self.closed = true;
    }

    fn committed_location(&self) -> Option<ozone::KeyLocation> {
        if self.is_empty() || !self.closed {
            None
        } else {
            let mut location = self.location.clone();
            location.length = self.progress.written;
            Some(location)
        }
    }
}

pub(crate) struct BlockEntryPool {
    blocks: Vec<WritableBlock>,
    current_index: usize,
    block_size: u64,
}

impl BlockEntryPool {
    pub(crate) fn new(locations: Vec<ozone::KeyLocation>, block_size: u64) -> Self {
        Self {
            blocks: locations
                .into_iter()
                .map(|location| WritableBlock::new(location, block_size))
                .collect(),
            current_index: 0,
            block_size,
        }
    }

    pub(crate) fn push_block(&mut self, location: ozone::KeyLocation) {
        self.blocks
            .push(WritableBlock::new(location, self.block_size));
    }

    pub(crate) fn current_block(&self) -> Option<&WritableBlock> {
        self.blocks.get(self.current_index)
    }

    pub(crate) fn current_block_mut(&mut self) -> Option<&mut WritableBlock> {
        self.blocks.get_mut(self.current_index)
    }

    pub(crate) fn current_location(&self) -> Option<&ozone::KeyLocation> {
        self.current_block().map(|block| &block.location)
    }

    pub(crate) fn needs_current_allocation(&self) -> bool {
        self.current_index >= self.blocks.len()
    }

    pub(crate) fn needs_lookahead(&self) -> bool {
        !self.needs_current_allocation() && self.spare_count() == 0
    }

    fn spare_count(&self) -> usize {
        self.blocks
            .len()
            .saturating_sub(self.current_index.saturating_add(1))
    }

    pub(crate) fn advance_past_full_current(&mut self) {
        if self.current_is_full() {
            self.current_index += 1;
        }
    }

    pub(crate) fn current_is_full(&self) -> bool {
        self.current_block().is_some_and(WritableBlock::is_full)
    }

    pub(crate) fn committed_locations(&self) -> Vec<ozone::KeyLocation> {
        self.blocks
            .iter()
            .filter_map(WritableBlock::committed_location)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{ActiveBlockProgress, BlockEntryPool, WritableBlock};
    use crate::proto::hadoop::{hdds, ozone};

    fn location(local_id: i64, len: u64) -> ozone::KeyLocation {
        ozone::KeyLocation {
            block_id: hdds::BlockId {
                container_block_id: hdds::ContainerBlockId {
                    container_id: 1,
                    local_id,
                },
                block_commit_sequence_id: Some(0),
            },
            offset: 0,
            length: len,
            create_version: Some(1),
            token: None,
            pipeline: None,
            part_number: None,
        }
    }

    #[test]
    fn active_block_progress_caps_writes_at_remaining_capacity() {
        let mut progress = ActiveBlockProgress::bounded(10);

        assert_eq!(progress.next_write_len(6), 6);
        progress.observe_write(6);
        assert_eq!(progress.next_write_len(10), 4);
        progress.observe_write(4);

        assert!(progress.is_full());
        assert_eq!(progress.next_write_len(1), 0);
    }

    #[test]
    fn active_block_progress_caps_zero_length_locations_at_block_size() {
        let mut progress = ActiveBlockProgress::from_location_len(0, 16);

        assert_eq!(progress.next_write_len(16), 16);
        progress.observe_write(16);

        assert!(progress.is_full());
        assert_eq!(progress.next_write_len(8), 0);
    }

    #[test]
    fn active_block_progress_prefers_nonzero_location_len() {
        let mut progress = ActiveBlockProgress::from_location_len(8, 16);

        assert_eq!(progress.next_write_len(16), 8);
        progress.observe_write(8);

        assert!(progress.is_full());
    }

    #[tokio::test]
    async fn writable_block_closes_empty_block_without_writer() {
        let mut block = WritableBlock::new(location(1, 8), 8);

        block.close().await.unwrap();

        assert!(block.committed_location().is_none());
    }

    #[test]
    fn block_entry_pool_advances_to_preallocated_spare_without_allocation() {
        let mut pool = BlockEntryPool::new(vec![location(1, 8), location(2, 8)], 8);

        assert_eq!(
            pool.current_location()
                .unwrap()
                .block_id
                .container_block_id
                .local_id,
            1
        );
        assert!(!pool.needs_current_allocation());
        assert!(!pool.needs_lookahead());

        pool.current_block_mut().unwrap().observe_write(8);
        pool.advance_past_full_current();

        assert_eq!(
            pool.current_location()
                .unwrap()
                .block_id
                .container_block_id
                .local_id,
            2
        );
        assert!(!pool.needs_current_allocation());
        assert!(pool.needs_lookahead());
    }

    #[test]
    fn block_entry_pool_commits_only_non_empty_entries() {
        let mut pool = BlockEntryPool::new(vec![location(1, 8), location(2, 8), location(3, 8)], 8);

        pool.current_block_mut().unwrap().observe_write(8);
        pool.current_block_mut()
            .unwrap()
            .update_committed_location(location(1, 8));
        pool.advance_past_full_current();

        let committed = pool.committed_locations();

        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].block_id.container_block_id.local_id, 1);
    }
}
