use crate::error::Result;
use crate::proto::hadoop::hdds;
use crate::ratis_stream::UnorderedRequestManager;
use std::sync::{atomic::AtomicU64, Arc};
use uuid::Uuid;

#[derive(Clone)]
pub struct RatisClient {
    client_id: Uuid,
    next_call_id: Arc<AtomicU64>,
    host_override: Option<String>,
}

impl Default for RatisClient {
    fn default() -> Self {
        Self {
            client_id: Uuid::new_v4(),
            next_call_id: Arc::new(AtomicU64::new(1)),
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

    pub async fn open_unordered_stream(
        &self,
        pipeline: &hdds::Pipeline,
    ) -> Result<UnorderedRequestManager> {
        UnorderedRequestManager::connect(
            self.client_id,
            Arc::clone(&self.next_call_id),
            pipeline,
            self.host_override.clone(),
        )
        .await
    }
}
