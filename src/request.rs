use std::{
    io::{self, Read},
    net::TcpStream,
};

use uuid::Uuid;

use crate::reader::{Reader, io_invalid_data};

pub struct Request {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub body: RequestBody,
}

pub struct DescribeTopicPartitionsRequest {
    pub topics: Vec<String>,
}

pub struct FetchPartition {
    pub index: i32,
    pub fetch_offset: i64,
}
pub struct FetchTopic {
    pub topic_id: Uuid,
    pub partitions: Vec<FetchPartition>,
}
pub struct FetchRequest {
    pub topics: Vec<FetchTopic>,
}

pub struct ProduceRequest {
    pub transactional_id: Option<String>,
    pub required_acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<ProduceTopic>,
}

pub struct ProduceTopic {
    pub name: String,
    pub partitions: Vec<ProducePartition>,
}

pub struct ProducePartition {
    pub index: i32,
    pub records: Option<Vec<u8>>,
}

pub enum RequestBody {
    ApiVersions,
    DescribeTopicPartitions(DescribeTopicPartitionsRequest),
    Fetch(FetchRequest),
    Produce(ProduceRequest),
}
pub fn parse_request(stream: &mut TcpStream) -> io::Result<Request> {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf)?;
    let msg_size = i32::from_be_bytes(size_buf) as usize;

    let mut buf = vec![0u8; msg_size];
    stream.read_exact(&mut buf)?;

    let mut reader = Reader::new(&buf);

    let api_key = reader.read_i16()?;
    let api_version = reader.read_i16()?;
    let correlation_id = reader.read_i32()?;
    let _client_id = reader.read_nullable_string()?;
    reader.read_tag_buffer()?;
    let body = match api_key {
        0 => {
            let req = parse_produce(&mut reader)?;
            RequestBody::Produce(req)
        }
        1 => {
            let req = parse_fetch(&mut reader)?;
            RequestBody::Fetch(req)
        }
        18 => RequestBody::ApiVersions,
        75 => {
            let req = parse_describe_topic_partitions(&mut reader)?;
            RequestBody::DescribeTopicPartitions(req)
        }
        _ => todo!(),
    };
    Ok(Request {
        api_key,
        api_version,
        correlation_id,
        body,
    })
}

fn parse_describe_topic_partitions(
    reader: &mut Reader,
) -> io::Result<DescribeTopicPartitionsRequest> {
    let array_len = reader.read_uvarint()?.saturating_sub(1) as usize;
    let mut topics: Vec<String> = Vec::with_capacity(array_len);
    for _ in 0..array_len {
        topics.push(reader.read_compact_string()?);
        reader.read_tag_buffer()?; // topic tag buffer
    }
    reader.read_tag_buffer()?; // request tag buffer
    Ok(DescribeTopicPartitionsRequest { topics })
}

/* FetchRequest V16
max_wait_ms => INT32
min_bytes => INT32
max_bytes => INT32
isolation_level => INT8
session_id => INT32
session_epoch => INT32
topics => topic_id [partitions]
  topic_id => UUID
  partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes
    partition => INT32
    current_leader_epoch => INT32
    fetch_offset => INT64
    last_fetched_epoch => INT32
    log_start_offset => INT64
    partition_max_bytes => INT32
forgotten_topics_data => topic_id [partitions]
  topic_id => UUID
  partitions => INT32
rack_id => COMPACT_STRING
cluster_id<tag: 0> => COMPACT_NULLABLE_STRING
replica_state<tag: 1> => replica_id replica_epoch
  replica_id => INT32
  replica_epoch => INT64
*/
fn parse_fetch(reader: &mut Reader) -> io::Result<FetchRequest> {
    reader.read_i32()?; // max_wait_ms
    reader.read_i32()?; // min_bytes
    reader.read_i32()?; // max_bytes
    reader.read_i8()?; // isolation_level
    reader.read_i32()?; // session id
    reader.read_i32()?; // session_epoch
    let length = reader.read_compact_array_len()?;
    let mut topics = Vec::with_capacity(length);
    for _ in 0..length {
        let topic_id = reader.read_uuid()?;

        let partition_length = reader.read_compact_array_len()?;
        let mut partitions = Vec::with_capacity(partition_length);
        for _ in 0..partition_length {
            let index = reader.read_i32()?; // partition
            reader.read_i32()?; // current_leader_epoch
            let fetch_offset = reader.read_i64()?; // fetch_offset
            reader.read_i32()?; // last_fetched_epoch
            reader.read_i64()?; // log_start_offset
            reader.read_i32()?; // partition_max_bytes
            partitions.push(FetchPartition {
                index,
                fetch_offset,
            });
        }

        reader.read_tag_buffer()?; // topic tagged fields

        topics.push(FetchTopic {
            topic_id,
            partitions,
        });
    }
    Ok(FetchRequest { topics })
}

fn parse_produce(reader: &mut Reader) -> io::Result<ProduceRequest> {
    let transactional_id = reader.read_compact_nullable_string()?;
    let required_acks = reader.read_i16()?;
    let timeout_ms = reader.read_i32()?;

    let topic_length = reader.read_compact_array_len()?;
    let mut topics = Vec::with_capacity(topic_length);

    for _ in 0..topic_length {
        let name = reader.read_compact_string()?;

        let partition_length = reader.read_compact_array_len()?;
        let mut partitions = Vec::with_capacity(partition_length);

        for _ in 0..partition_length {
            let index = reader.read_i32()?;
            let records = reader.read_compact_nullable_bytes()?;

            reader.read_tag_buffer()?; // partition tagged fields

            partitions.push(ProducePartition { index, records });
        }

        reader.read_tag_buffer()?; // topic tagged fields

        topics.push(ProduceTopic { name, partitions });
    }

    reader.read_tag_buffer()?; // request tagged fields

    Ok(ProduceRequest {
        transactional_id,
        required_acks,
        timeout_ms,
        topics,
    })
}
