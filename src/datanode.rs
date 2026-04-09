use crate::error::{Error, Result};
use crate::proto::hadoop::hdds;
use crate::proto::hadoop::hdds::datanode;
use crate::proto::hadoop::hdds::datanode::xceiver_client_protocol_service_client::XceiverClientProtocolServiceClient;
use crate::util::{
    block_id_to_datanode, datanode_standalone_address, datanode_uuid_string, latest_key_locations,
    normalize_endpoint, CLIENT_VERSION, MAX_GRPC_MESSAGE_SIZE,
};
use bytes::BytesMut;
use tokio_stream::iter;
use tonic::transport::Channel;

pub struct DatanodeClient {
    response_data_size: u32,
    host_override: Option<String>,
}

impl DatanodeClient {
    pub fn new(response_data_size: u32, host_override: Option<String>) -> Self {
        Self {
            response_data_size,
            host_override,
        }
    }

    pub async fn read_key_blocks(
        &self,
        key_info: &crate::proto::hadoop::ozone::KeyInfo,
    ) -> Result<Vec<u8>> {
        let locations = latest_key_locations(key_info);
        let mut bytes = Vec::with_capacity(key_info.data_size as usize);
        for location in locations {
            bytes.extend(self.read_block(&location).await?);
        }
        Ok(bytes)
    }

    pub async fn read_block(
        &self,
        location: &crate::proto::hadoop::ozone::KeyLocation,
    ) -> Result<Vec<u8>> {
        let pipeline = location
            .pipeline
            .as_ref()
            .ok_or(Error::MissingField("key_location.pipeline"))?;

        let mut last_error = None;
        for node in &pipeline.members {
            match self.read_block_from_node(node, location).await {
                Ok(bytes) => return Ok(bytes),
                Err(err) => last_error = Some(err),
            }
        }

        Err(last_error
            .unwrap_or_else(|| Error::InvalidState("no datanodes in pipeline".to_string())))
    }

    async fn read_block_from_node(
        &self,
        node: &hdds::DatanodeDetailsProto,
        location: &crate::proto::hadoop::ozone::KeyLocation,
    ) -> Result<Vec<u8>> {
        let address = datanode_standalone_address(node, self.host_override.as_deref())?;
        let channel = Channel::from_shared(normalize_endpoint(&address))
            .map_err(|e| Error::InvalidState(format!("invalid datanode endpoint: {e}")))?
            .connect()
            .await?;
        let mut client = XceiverClientProtocolServiceClient::new(channel)
            .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE);

        let block_id = &location.block_id;
        #[allow(deprecated)]
        let request = datanode::ContainerCommandRequestProto {
            cmd_type: datanode::Type::ReadBlock as i32,
            trace_id: None,
            container_id: block_id.container_block_id.container_id,
            datanode_uuid: datanode_uuid_string(node)?,
            pipeline_id: None,
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
            write_chunk: None,
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
            read_block: Some(datanode::ReadBlockRequestProto {
                block_id: block_id_to_datanode(block_id, None)?,
                offset: 0,
                length: Some(location.length),
                response_data_size: Some(self.response_data_size),
            }),
        };

        let response = client.send(iter(vec![request])).await?;
        let mut stream = response.into_inner();
        let mut data = BytesMut::with_capacity(location.length as usize);

        while let Some(response) = stream.message().await? {
            let result = datanode::Result::try_from(response.result)
                .unwrap_or(datanode::Result::ContainerInternalError);
            if result != datanode::Result::Success {
                return Err(Error::Datanode {
                    result,
                    message: response
                        .message
                        .unwrap_or_else(|| "datanode read failed without message".to_string()),
                });
            }

            let read = response
                .read_block
                .ok_or(Error::MissingField("read_block response body"))?;
            data.extend_from_slice(&read.data);
        }

        Ok(data.to_vec())
    }
}
