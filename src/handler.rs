use std::{io::Write, net::TcpStream, sync::Arc};

use crate::{
    metadata::Metadata,
    request::{Request, RequestBody, parse_request},
    response::{
        ApiKey, ApiVersionsResponse, DescribeTopicPartitionsResponse, PartitionMetadata, Response,
        ResponseBody, TopicMetadata,
    },
};

pub struct Handler {
    metadata: Arc<Metadata>,
}

impl Handler {
    pub fn new(metadata: Arc<Metadata>) -> Self {
        Self { metadata }
    }
    pub fn handle(&self, mut stream: TcpStream) {
        loop {
            let request = match parse_request(&mut stream) {
                Ok(req) => req,
                Err(e) => {
                    eprintln!("Error: {e}");
                    break;
                }
            };
            let response = self.handle_request(&request);
            if let Err(e) = stream.write_all(&response.serialize()) {
                eprintln!("Failed to write response: {e}");
                break;
            }
        }
    }
    fn handle_request(&self, request: &Request) -> Response {
        match &request.body {
            RequestBody::ApiVersions => self.handle_api_version(request),
            RequestBody::DescribeTopicPartitions(_) => {
                self.handle_describe_topic_partitions(request)
            }
        }
    }
    fn handle_api_version(&self, request: &Request) -> Response {
        let error_code = if (0..=4).contains(&request.api_version) {
            0i16
        } else {
            35i16
        };
        Response {
            correlation_id: request.correlation_id,
            body: ResponseBody::ApiVersions(ApiVersionsResponse {
                error_code,
                api_keys: vec![
                    ApiKey {
                        api_key: 18,
                        min_version: 0,
                        max_version: 4,
                    },
                    ApiKey {
                        api_key: 75,
                        min_version: 0,
                        max_version: 0,
                    },
                ],
                throttle_time_ms: 0,
            }),
        }
    }
    fn handle_describe_topic_partitions(&self, request: &Request) -> Response {
        let RequestBody::DescribeTopicPartitions(body) = &request.body else {
            unreachable!()
        };
        let topics: Vec<TopicMetadata> = body
            .topics
            .iter()
            .map(|topic_name| {
                let id = self.metadata.topic_names.get(topic_name);
                match id {
                    Some(id) => {
                        let topic = self.metadata.topics.get(&id).unwrap();
                        let mut partitions: Vec<PartitionMetadata> = topic
                            .partitions
                            .iter()
                            .map(|(&id, partition)| (id, partition).into())
                            .collect();
                        partitions.sort_by_key(|p| p.partition_index);
                        TopicMetadata {
                            error_code: 0,
                            topic_name: topic_name.clone(),
                            topic_id: id.as_bytes().clone(),
                            is_internal: true, // is this correct?
                            partitions,
                            topic_authorized_operations: 0, // is this correct?
                        }
                    }
                    None => TopicMetadata {
                        error_code: 3,
                        topic_name: topic_name.clone(),
                        topic_id: [0u8; 16],
                        is_internal: false,
                        partitions: vec![],
                        topic_authorized_operations: 0,
                    },
                }
            })
            .collect();
        Response {
            correlation_id: request.correlation_id,
            body: ResponseBody::DescribeTopicPartitions(DescribeTopicPartitionsResponse {
                throttle_time_ms: 0,
                topics,
                next_cursor: -1,
            }),
        }
    }
}
