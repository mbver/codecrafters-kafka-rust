use std::{
    io::{self, Write},
    net::TcpStream,
    path::Path,
    sync::Arc,
};

use crate::{
    metadata::Metadata,
    request::{Request, RequestBody, parse_request},
    response::{
        ApiKey, ApiVersionsResponse, DescribeTopicPartitionsResponse, FetchResponse,
        FetchedPartition, FetchedTopic, PartitionMetadata, Response, ResponseBody, TopicMetadata,
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
            let response = match self.handle_request(&request) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("Error: {e}");
                    continue;
                }
            };
            if let Err(e) = stream.write_all(&response.serialize()) {
                eprintln!("Failed to write response: {e}");
                break;
            }
        }
    }
    fn handle_request(&self, request: &Request) -> io::Result<Response> {
        match &request.body {
            RequestBody::ApiVersions => Ok(self.handle_api_version(request)),
            RequestBody::DescribeTopicPartitions(_) => {
                Ok(self.handle_describe_topic_partitions(request))
            }
            RequestBody::Fetch(_) => self.handle_fetch(request),
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
                        api_key: 1,
                        min_version: 0,
                        max_version: 16,
                    },
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
        let mut topics: Vec<TopicMetadata> = body
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
                            is_internal: topic_name.starts_with("__"),
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
        topics.sort_by_key(|t| t.topic_name.clone());
        Response {
            correlation_id: request.correlation_id,
            body: ResponseBody::DescribeTopicPartitions(DescribeTopicPartitionsResponse {
                throttle_time_ms: 0,
                topics,
                next_cursor: -1,
            }),
        }
    }
    fn handle_fetch(&self, request: &Request) -> io::Result<Response> {
        let RequestBody::Fetch(body) = &request.body else {
            unreachable!()
        };
        let mut responses = Vec::new();
        for fetch_topic in &body.topics {
            let partitions = match self.metadata.topics.get(&fetch_topic.topic_id) {
                Some(topic) => {
                    let mut partitions = Vec::with_capacity(fetch_topic.partitions.len());
                    for p in &fetch_topic.partitions {
                        let partition = if topic.partitions.contains_key(&p.index) {
                            let log_dir = Path::new("/tmp/kraft-combined-logs");
                            FetchedPartition {
                                index: p.index,
                                error_code: 0,
                                records: Some(read_partition_records(
                                    log_dir,
                                    &topic.name,
                                    p.index,
                                    p.fetch_offset,
                                )?),
                            }
                        } else {
                            FetchedPartition {
                                index: p.index,
                                error_code: 3,
                                records: None,
                            }
                        };
                        partitions.push(partition);
                    }
                    partitions
                }
                None => fetch_topic
                    .partitions
                    .iter()
                    .map(|p| FetchedPartition {
                        index: p.index,
                        error_code: 100,
                        records: None,
                    })
                    .collect(),
            };
            responses.push(FetchedTopic {
                topic_id: fetch_topic.topic_id,
                partitions,
            });
        }
        Ok(Response {
            correlation_id: request.correlation_id,
            body: ResponseBody::Fetch(FetchResponse {
                throttle_time: 0,
                error_code: 0,
                session_id: 0,
                responses,
            }),
        })
    }
}

fn read_partition_records(
    log_dir: &Path,
    topic_name: &str,
    partition: i32,
    fetch_offset: i64,
) -> io::Result<Vec<u8>> {
    let path = log_dir
        .join(format!("{topic_name}-{partition}"))
        .join("00000000000000000000.log");

    let bytes = std::fs::read(path)?;

    // TODO: filter RecordBatches by fetch_offset.
    Ok(bytes)
}
