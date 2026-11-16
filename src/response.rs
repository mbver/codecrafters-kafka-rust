use uuid::Uuid;

use crate::metadata::Partition;

struct Writer {
    buf: Vec<u8>,
}
impl Writer {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }
    fn write_u8(&mut self, value: u8) {
        self.buf.push(value);
    }
    fn write_i16(&mut self, value: i16) {
        self.buf.extend_from_slice(&value.to_be_bytes());
    }
    fn write_i32(&mut self, value: i32) {
        self.buf.extend_from_slice(&value.to_be_bytes());
    }
    pub fn write_i64(&mut self, value: i64) {
        self.buf.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_compact_nullable_bytes(&mut self, value: Option<&[u8]>) {
        match value {
            Some(bytes) => {
                self.write_uvarint((bytes.len() + 1) as u32);
                self.buf.extend_from_slice(bytes);
            }
            None => self.write_uvarint(0),
        }
    }
    fn write_bytes(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }
    // only 7 bits in a byte used. bit 7 indicates whether next byte follows
    // 0x80 = 10000000
    fn write_uvarint(&mut self, mut value: u32) {
        while value >= 0x80 {
            self.buf.push((value as u8) | 0x80);
            value >>= 7;
        }
        self.buf.push(value as u8);
    }
    fn write_compact_array_len(&mut self, len: usize) {
        self.write_uvarint((len + 1) as u32);
    }
    fn write_compact_string(&mut self, value: &str) {
        self.write_uvarint((value.len() + 1) as u32);
        self.write_bytes(value.as_bytes());
    }
    fn write_compact_nullable_string(&mut self, value: Option<&str>) {
        self.write_compact_nullable_bytes(value.map(str::as_bytes));
    }
    fn write_tag_buffer(&mut self) {
        self.write_u8(0);
    }
    fn into_inner(self) -> Vec<u8> {
        self.buf
    }
    fn write_i8(&mut self, value: i8) {
        self.buf.push(value as u8);
    }

    fn write_bool(&mut self, value: bool) {
        self.buf.push(u8::from(value));
    }

    fn write_uuid(&mut self, value: &[u8; 16]) {
        self.write_bytes(value);
    }
}
pub struct Response {
    pub correlation_id: i32,
    pub body: ResponseBody,
}

pub enum ResponseBody {
    ApiVersions(ApiVersionsResponse),
    DescribeTopicPartitions(DescribeTopicPartitionsResponse),
    Fetch(FetchResponse),
    Produce(ProduceResponse),
}

impl ResponseBody {
    fn serialize_into(&self, writer: &mut Writer) {
        match self {
            ResponseBody::ApiVersions(body) => {
                body.serialize_into(writer);
            }
            ResponseBody::DescribeTopicPartitions(body) => {
                body.serialize_into(writer);
            }
            ResponseBody::Fetch(body) => {
                body.serialize_into(writer);
            }
            ResponseBody::Produce(body) => {
                body.serialize_into(writer);
            }
        }
    }
}

pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_keys: Vec<ApiKey>,
    pub throttle_time_ms: i32,
}

pub struct ApiKey {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

impl Response {
    pub fn serialize(&self) -> Vec<u8> {
        let mut writer = Writer::new();

        writer.write_i32(0); // reserve message size
        writer.write_i32(self.correlation_id);

        self.body.serialize_into(&mut writer);

        let mut buf = writer.into_inner();
        let msg_size = (buf.len() - 4) as i32;
        buf[..4].copy_from_slice(&msg_size.to_be_bytes());
        buf
    }
}

impl ApiVersionsResponse {
    fn serialize_into(&self, writer: &mut Writer) {
        writer.write_i16(self.error_code);

        writer.write_compact_array_len(self.api_keys.len());
        for api_key in &self.api_keys {
            writer.write_i16(api_key.api_key);
            writer.write_i16(api_key.min_version);
            writer.write_i16(api_key.max_version);
            writer.write_tag_buffer();
        }

        writer.write_i32(self.throttle_time_ms);
        writer.write_tag_buffer();
    }
}

pub struct DescribeTopicPartitionsResponse {
    pub throttle_time_ms: i32,
    pub topics: Vec<TopicMetadata>,
    pub next_cursor: i8,
}

pub struct TopicMetadata {
    pub error_code: i16,
    pub topic_name: String,
    pub topic_id: [u8; 16],
    pub is_internal: bool,
    pub partitions: Vec<PartitionMetadata>,
    pub topic_authorized_operations: i32,
}

impl DescribeTopicPartitionsResponse {
    fn serialize_into(&self, writer: &mut Writer) {
        writer.write_tag_buffer();
        writer.write_i32(self.throttle_time_ms);

        writer.write_compact_array_len(self.topics.len());
        for topic in &self.topics {
            writer.write_i16(topic.error_code);
            writer.write_compact_string(&topic.topic_name);
            writer.write_uuid(&topic.topic_id);
            writer.write_bool(topic.is_internal);

            writer.write_compact_array_len(topic.partitions.len());
            for partition in &topic.partitions {
                partition.serialize_into(writer);
            }
            writer.write_i32(topic.topic_authorized_operations);
            writer.write_tag_buffer();
        }

        writer.write_i8(self.next_cursor);
        writer.write_tag_buffer();
    }
}

pub struct PartitionMetadata {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    pub eligible_leader_replicas: Vec<i32>,
    pub last_known_elr: Vec<i32>,
    pub offline_replicas: Vec<i32>,
}

impl From<(i32, &Partition)> for PartitionMetadata {
    fn from((partition_index, partition): (i32, &Partition)) -> Self {
        Self {
            error_code: 0,
            partition_index,
            leader_id: partition.leader,
            leader_epoch: partition.leader_epoch,
            replica_nodes: partition.replicas.clone(),
            isr_nodes: partition.isr.clone(),
            eligible_leader_replicas: vec![],
            last_known_elr: vec![],
            offline_replicas: vec![],
        }
    }
}

impl PartitionMetadata {
    fn serialize_into(&self, writer: &mut Writer) {
        writer.write_i16(self.error_code);
        writer.write_i32(self.partition_index);
        writer.write_i32(self.leader_id);
        writer.write_i32(self.leader_epoch);

        writer.write_compact_array_len(self.replica_nodes.len());
        for &node in &self.replica_nodes {
            writer.write_i32(node);
        }

        writer.write_compact_array_len(self.isr_nodes.len());
        for &node in &self.isr_nodes {
            writer.write_i32(node);
        }

        writer.write_compact_array_len(self.eligible_leader_replicas.len());
        for &node in &self.eligible_leader_replicas {
            writer.write_i32(node);
        }

        writer.write_compact_array_len(self.last_known_elr.len());
        for &node in &self.last_known_elr {
            writer.write_i32(node);
        }

        writer.write_compact_array_len(self.offline_replicas.len());
        for &node in &self.offline_replicas {
            writer.write_i32(node);
        }

        writer.write_tag_buffer();
    }
}
/*
* Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] [node_endpoints]<tag: 0>
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic_id [partitions]
    topic_id => UUID
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records diverging_epoch<tag: 0> current_leader<tag: 1> snapshot_id<tag: 2>
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset
        producer_id => INT64
        first_offset => INT64
      preferred_read_replica => INT32
      records => COMPACT_RECORDS
      diverging_epoch<tag: 0> => epoch end_offset
        epoch => INT32
        end_offset => INT64
      current_leader<tag: 1> => leader_id leader_epoch
        leader_id => INT32
        leader_epoch => INT32
      snapshot_id<tag: 2> => end_offset epoch
        end_offset => INT64
        epoch => INT32
  node_endpoints<tag: 0> => node_id host port rack
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING

*/
pub struct FetchResponse {
    pub throttle_time: i32,
    pub error_code: i16,
    pub session_id: i32,
    pub responses: Vec<FetchedTopic>,
}

pub struct FetchedTopic {
    pub topic_id: Uuid,
    pub partitions: Vec<FetchedPartition>,
}

pub struct FetchedPartition {
    pub index: i32,
    pub error_code: i16,
    pub records: Option<Vec<u8>>,
}

impl FetchResponse {
    fn serialize_into(&self, writer: &mut Writer) {
        writer.write_tag_buffer();
        writer.write_i32(self.throttle_time);
        writer.write_i16(self.error_code);
        writer.write_i32(self.session_id);
        writer.write_compact_array_len(self.responses.len());
        for topic in &self.responses {
            topic.serialize_into(writer);
            writer.write_tag_buffer();
        }
        writer.write_tag_buffer();
    }
}

impl FetchedTopic {
    fn serialize_into(&self, writer: &mut Writer) {
        writer.write_uuid(&self.topic_id.into_bytes());
        writer.write_compact_array_len(self.partitions.len());
        for partition in &self.partitions {
            partition.serialize_into(writer);
            writer.write_tag_buffer();
        }
    }
}

impl FetchedPartition {
    fn serialize_into(&self, writer: &mut Writer) {
        writer.write_i32(self.index);
        writer.write_i16(self.error_code);
        writer.write_i64(-1); // high_watermark
        writer.write_i64(-1); // last_stable_offset
        writer.write_i64(-1); // log_start_offset
        writer.write_compact_array_len(0); // aborted_transactions
        writer.write_i32(-1); // preferred_read_replica
        writer.write_compact_nullable_bytes(self.records.as_deref()); // records
        writer.write_tag_buffer();
    }
}

pub struct ProduceResponse {
    pub topics: Vec<ProduceTopic>,
    pub throttle_time_ms: i32,
}

pub struct ProduceTopic {
    pub name: String,
    pub partitions: Vec<ProducePartition>,
}

pub struct ProducePartition {
    pub index: i32,
    pub error_code: i16,
    pub base_offset: i64,
    pub log_append_time_ms: i64,
    pub log_start_offset: i64,
    pub record_errors: Vec<RecordError>,
    pub error_message: Option<String>,
}

pub struct RecordError {
    pub batch_index: i32,
    pub batch_index_error_message: Option<String>,
}

impl ProduceResponse {
    fn serialize_into(&self, writer: &mut Writer) {
        writer.write_tag_buffer();
        writer.write_compact_array_len(self.topics.len());

        for topic in &self.topics {
            topic.serialize_into(writer);
        }

        writer.write_i32(self.throttle_time_ms);

        // node_endpoints tagged field (tag 0) omitted.
        writer.write_tag_buffer();
    }
}

impl ProduceTopic {
    fn serialize_into(&self, writer: &mut Writer) {
        writer.write_compact_string(&self.name);

        writer.write_compact_array_len(self.partitions.len());
        for partition in &self.partitions {
            partition.serialize_into(writer);
        }

        writer.write_tag_buffer();
    }
}

impl ProducePartition {
    fn serialize_into(&self, writer: &mut Writer) {
        writer.write_i32(self.index);
        writer.write_i16(self.error_code);
        writer.write_i64(self.base_offset);
        writer.write_i64(self.log_append_time_ms);
        writer.write_i64(self.log_start_offset);

        writer.write_compact_array_len(self.record_errors.len());
        for error in &self.record_errors {
            error.serialize_into(writer);
        }

        writer.write_compact_nullable_string(self.error_message.as_deref());

        // current_leader tagged field (tag 0) omitted.
        writer.write_tag_buffer();
    }
}

impl RecordError {
    fn serialize_into(&self, writer: &mut Writer) {
        writer.write_i32(self.batch_index);

        writer.write_compact_nullable_string(self.batch_index_error_message.as_deref());

        writer.write_tag_buffer();
    }
}
