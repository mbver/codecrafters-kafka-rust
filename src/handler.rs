use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    net::TcpStream,
    path::Path,
    sync::Arc,
};

use crate::{
    metadata::Metadata,
    request::{Request, RequestBody, parse_request},
    response::{
        ApiKey, ApiVersionsResponse, DescribeTopicPartitionsResponse, FetchResponse,
        FetchedPartition, FetchedTopic, PartitionMetadata, ProducePartition, ProduceResponse,
        ProduceTopic, Response, ResponseBody, TopicMetadata,
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
            RequestBody::Produce(_) => self.handle_produce(request),
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
                    ApiKey {
                        api_key: 0,
                        min_version: 0,
                        max_version: 11,
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
    fn handle_produce(&self, request: &Request) -> io::Result<Response> {
        let RequestBody::Produce(body) = &request.body else {
            unreachable!()
        };

        let mut topics = Vec::with_capacity(body.topics.len());

        for produce_topic in &body.topics {
            let mut partitions = Vec::with_capacity(produce_topic.partitions.len());

            let topic = self
                .metadata
                .topic_names
                .get(&produce_topic.name)
                .and_then(|id| self.metadata.topics.get(id));

            for produce_partition in &produce_topic.partitions {
                let partition_response = match topic {
                    Some(topic) if topic.partitions.contains_key(&produce_partition.index) => {
                        match &produce_partition.records {
                            Some(records) => {
                                let log_dir = Path::new("/tmp/kraft-combined-logs");

                                let base_offset = append_record_batches(
                                    log_dir,
                                    &produce_topic.name,
                                    produce_partition.index,
                                    records,
                                )?;

                                ProducePartition {
                                    index: produce_partition.index,
                                    error_code: 0,
                                    base_offset,
                                    log_append_time_ms: -1,
                                    log_start_offset: 0,
                                    record_errors: vec![],
                                    error_message: None,
                                }
                            }
                            None => ProducePartition {
                                index: produce_partition.index,
                                error_code: 87, // INVALID_RECORD
                                base_offset: -1,
                                log_append_time_ms: -1,
                                log_start_offset: -1,
                                record_errors: vec![],
                                error_message: Some("records is null".to_string()),
                            },
                        }
                    }
                    _ => ProducePartition {
                        index: produce_partition.index,
                        error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
                        base_offset: -1,
                        log_append_time_ms: -1,
                        log_start_offset: -1,
                        record_errors: vec![],
                        error_message: None,
                    },
                };

                partitions.push(partition_response);
            }

            topics.push(ProduceTopic {
                name: produce_topic.name.clone(),
                partitions,
            });
        }

        Ok(Response {
            correlation_id: request.correlation_id,
            body: ResponseBody::Produce(ProduceResponse {
                topics,
                throttle_time_ms: 0,
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

// the file might already have several record batches
// we will read each batch's header to compute the next_offset
// we also skip reading the batch and seek to the end for next batch or EOF
// the loop terminate at EOF
fn read_next_offset(path: &Path) -> io::Result<i64> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };

    let mut reader = BufReader::new(file);
    let mut next_offset = 0i64;

    loop {
        let mut header = [0u8; 12];

        if reader.read(&mut header[..1])? == 0 {
            // EOF, terminate
            break;
        }

        reader.read_exact(&mut header[1..])?;

        let base_offset = i64::from_be_bytes(header[..8].try_into().unwrap());
        let batch_length = i32::from_be_bytes(header[8..12].try_into().unwrap());

        if batch_length < 15 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid record batch length",
            ));
        }

        let mut details = [0u8; 15];
        reader.read_exact(&mut details)?;

        let last_offset_delta = i32::from_be_bytes(details[11..15].try_into().unwrap());

        next_offset = next_offset.max(base_offset + last_offset_delta as i64 + 1);

        reader.seek(SeekFrom::Current((batch_length - 15) as i64))?;
    }

    Ok(next_offset)
}

// the records have the messages but it doesn't have the base_offset for each record_batch
// we will read through it to assign base_offset to each record_batch
// after all record_batch have base_offset, we write records to the end of the file
fn append_record_batches(
    log_dir: &Path,
    topic_name: &str,
    partition: i32,
    records: &[u8],
) -> io::Result<i64> {
    let partition_dir = log_dir.join(format!("{topic_name}-{partition}"));
    fs::create_dir_all(&partition_dir)?;

    let path = partition_dir.join("00000000000000000000.log");

    let mut next_offset = read_next_offset(&path)?;
    let base_offset = next_offset;

    let mut records = records.to_vec();
    let mut position = 0;

    while position < records.len() {
        // each batch need a 27-byte header
        if records.len() - position < 27 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "incomplete record batch",
            ));
        }

        let batch_length =
            i32::from_be_bytes(records[position + 8..position + 12].try_into().unwrap());
        if batch_length < 15 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid record batch length",
            ));
        }

        let batch_size = 12 + batch_length as usize;

        if batch_size > records.len() - position {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "incomplete record batch",
            ));
        }

        let last_offset_delta =
            i32::from_be_bytes(records[position + 23..position + 27].try_into().unwrap()); // how many records in the batch

        // Assign the broker's base offset.
        records[position..position + 8].copy_from_slice(&next_offset.to_be_bytes());

        next_offset += last_offset_delta as i64 + 1;
        position += batch_size;
    }

    let mut file = OpenOptions::new().create(true).append(true).open(path)?;

    file.write_all(&records)?;
    file.flush()?;

    Ok(base_offset)
}
