pub struct Response {
    pub correlation_id: i32,
    pub body: ResponseBody,
}

pub enum ResponseBody {
    ApiVersions(ApiVersionsResponse),
    DescribeTopicPartitions(DescribeTopicPartitionsResponse),
}

impl ResponseBody {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            ResponseBody::ApiVersions(body) => {
                body.serialize_into(buf);
            }
            ResponseBody::DescribeTopicPartitions(body) => {
                body.serialize_into(buf);
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
        let mut buf = Vec::new();
        buf.extend_from_slice(&[0; 4]); // reserve msg size
        buf.extend_from_slice(&self.correlation_id.to_be_bytes());
        self.body.serialize_into(&mut buf);
        let msg_size = (buf.len() - 4) as i32;
        buf[..4].copy_from_slice(&msg_size.to_be_bytes());
        buf
    }
}

impl ApiVersionsResponse {
    pub fn serialize_into(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.error_code.to_be_bytes());
        buf.push((self.api_keys.len() + 1) as u8); // compact array: element_count +1
        for api_key in &self.api_keys {
            buf.extend_from_slice(&api_key.api_key.to_be_bytes());
            buf.extend_from_slice(&api_key.min_version.to_be_bytes());
            buf.extend_from_slice(&api_key.max_version.to_be_bytes());
            // empty tagged fields
            buf.push(0);
        }
        buf.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
        // empty tagged fields
        buf.push(0);
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
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        buf.push(0); // response header v1: empty TAG_BUFFER
        buf.push((self.topics.len() + 1) as u8); // compact array
        for topic in &self.topics {
            buf.extend_from_slice(&topic.error_code.to_be_bytes());
            buf.push((topic.topic_name.len() + 1) as u8); // compact string
            buf.extend_from_slice(topic.topic_name.as_bytes());
            buf.extend_from_slice(&topic.topic_id);
            buf.push(topic.is_internal as u8);
            buf.push((topic.partitions.len() + 1) as u8); // compact array
            buf.extend_from_slice(&topic.topic_authorized_operations.to_be_bytes());
            buf.push(0); // empty tag buffer
        }
        buf.push(self.next_cursor as u8);
        buf.push(0); // empty tag buffer
    }
}
pub struct PartitionMetadata;
