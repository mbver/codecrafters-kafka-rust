use std::{
    clone,
    collections::HashMap,
    fs::File,
    io::{self, BufRead, BufReader, Read},
};

use uuid::Uuid;

use crate::reader::{Reader, io_invalid_data};

struct RecordBatch {
    base_offset: i64,
    partition_leader_epoch: i32,
    attributes: i16,
    last_offset_delta: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records: Vec<Record>,
}

struct Record {
    attributes: i8,
    timestamp_delta: i64,
    offset_delta: i32,
    key: Option<Vec<u8>>,
    value: Option<MetadataRecord>,
}

enum MetadataRecord {
    FeatureLevel(FeatureLevelRecord),
    Topic(TopicRecord),
    Partition(PartitionRecord),
    // PartitionChange(PartitionChangeRecord),
}

struct FeatureLevelRecord {
    name: String,
    feature_level: i16,
}

struct TopicRecord {
    name: String,
    topic_id: Uuid,
}

struct PartitionRecord {
    partition_id: i32,
    topic_id: Uuid,
    replicas: Vec<i32>,
    isr: Vec<i32>,
    removing_replicas: Vec<i32>,
    adding_replicas: Vec<i32>,
    leader: i32,
    leader_epoch: i32,
    partition_epoch: i32,
    directories: Vec<Uuid>,
}

// the state container, holding all topics and partions
pub struct Metadata {
    pub topics: HashMap<Uuid, Topic>,
    pub topic_names: HashMap<String, Uuid>,
}

impl Metadata {
    fn new() -> Self {
        Self {
            topics: HashMap::new(),
            topic_names: HashMap::new(),
        }
    }

    fn apply(&mut self, record: Record) -> io::Result<()> {
        match record.value {
            Some(MetadataRecord::FeatureLevel(_)) => {}

            Some(MetadataRecord::Topic(topic)) => {
                if let Some(entry) = self.topics.get_mut(&topic.topic_id) {
                    self.topic_names.remove(&entry.name); // if new topic has the same uuid but different name, remove the old name because it is stale
                    entry.name = topic.name.clone();
                } else {
                    self.topics.insert(
                        topic.topic_id,
                        Topic {
                            name: topic.name.clone(),
                            partitions: HashMap::new(),
                        },
                    );
                }

                self.topic_names.insert(topic.name, topic.topic_id);
            }

            Some(MetadataRecord::Partition(partition)) => {
                let topic = self
                    .topics
                    .get_mut(&partition.topic_id)
                    .ok_or_else(|| io_invalid_data("partition references unknown topic"))?;

                topic.partitions.insert(
                    partition.partition_id,
                    Partition {
                        replicas: partition.replicas,
                        isr: partition.isr,
                        removing_replicas: partition.removing_replicas,
                        adding_replicas: partition.adding_replicas,
                        leader: partition.leader,
                        leader_epoch: partition.leader_epoch,
                        partition_epoch: partition.partition_epoch,
                        directories: partition.directories,
                    },
                );
            }
            None => {}
        }

        Ok(())
    }
}

pub struct Topic {
    pub name: String,
    pub partitions: HashMap<i32, Partition>,
}

#[derive(Clone)]
pub struct Partition {
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub removing_replicas: Vec<i32>,
    pub adding_replicas: Vec<i32>,
    pub leader: i32,
    pub leader_epoch: i32,
    pub partition_epoch: i32,
    pub directories: Vec<Uuid>,
}

pub fn replay<R: Read>(reader: R) -> io::Result<Metadata> {
    let mut reader = BufReader::new(reader);
    let mut metadata = Metadata::new();

    while let Some(batch) = read_batch(&mut reader)? {
        for record in batch.records {
            metadata.apply(record)?;
        }
    }

    Ok(metadata)
}

pub fn replay_file(path: &str) -> io::Result<Metadata> {
    let file = File::open(path)?;
    replay(file)
}

fn read_batch(file: &mut BufReader<impl Read>) -> io::Result<Option<RecordBatch>> {
    if file.fill_buf()?.is_empty() {
        return Ok(None);
    }
    let base_offset = read_i64(file)?;
    let batch_length = read_i32(file)?;
    if batch_length < 0 {
        return Err(io_invalid_data("negative batch length"));
    }

    let mut buf = vec![0; batch_length as usize];
    file.read_exact(&mut buf)?;
    let mut reader = Reader::new(&buf);

    let partition_leader_epoch = reader.read_i32()?;
    let _magic_byte = reader.read_i8()?;
    let _crc = reader.read_u32()?;
    let attributes = reader.read_i16()?;
    let last_offset_delta = reader.read_i32()?;
    let base_timestamp = reader.read_i64()?;
    let max_timestamp = reader.read_i64()?;
    let producer_id = reader.read_i64()?;
    let producer_epoch = reader.read_i16()?;
    let base_sequence = reader.read_i32()?;
    let records_count = reader.read_i32()?;
    if records_count < 0 {
        return Err(io_invalid_data("negative record count"));
    }

    let mut records = Vec::with_capacity(records_count as usize);
    for _ in 0..records_count {
        records.push(read_record(&mut reader)?)
    }
    if !reader.is_empty() {
        return Err(io_invalid_data("unconsumed batch bytes"));
    }
    Ok(Some(RecordBatch {
        base_offset,
        partition_leader_epoch,
        attributes,
        last_offset_delta,
        base_timestamp,
        max_timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
        records,
    }))
}

fn read_i64(file: &mut BufReader<impl Read>) -> io::Result<i64> {
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf)?;
    Ok(i64::from_be_bytes(buf))
}

fn read_i32(file: &mut BufReader<impl Read>) -> io::Result<i32> {
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

fn read_record(reader: &mut Reader<'_>) -> io::Result<Record> {
    let record_length = reader.read_varint()?;
    if record_length < 0 {
        return Err(io_invalid_data("negative record length"));
    }
    let record_buf = reader.read_bytes(record_length as usize)?;
    let mut record_reader = Reader::new(record_buf);

    let attributes = record_reader.read_i8()?;
    let timestamp_delta = record_reader.read_varlong()?;
    let offset_delta = record_reader.read_varint()?;
    let key_length = record_reader.read_varint()?;
    let key = match key_length {
        -1 => None,
        0 => Some(vec![]),
        n if n > 0 => Some(record_reader.read_bytes(n as usize)?.to_vec()),
        n => return Err(io_invalid_data(&format!("invalid key length: {n}"))),
    };

    let value_length = record_reader.read_varint()?;

    let value_buf = record_reader.read_bytes(value_length as usize)?;

    let mut value_reader = Reader::new(value_buf);

    let value = read_metadata_record(&mut value_reader)?;

    if !value_reader.is_empty() {
        return Err(io_invalid_data("unconsumed metadata record bytes"));
    }
    record_reader.skip_headers()?;
    if !record_reader.is_empty() {
        return Err(io_invalid_data("unconsumed record bytes"));
    }
    Ok(Record {
        attributes,
        timestamp_delta,
        offset_delta,
        key,
        value: Some(value),
    })
}
fn read_metadata_record(reader: &mut Reader<'_>) -> io::Result<MetadataRecord> {
    let _frame_version = reader.read_u8()?;
    let record_type = reader.read_u8()?;
    let _version = reader.read_u8()?;

    match record_type {
        0x0c => read_feature_level(reader),
        0x2 => read_topic(reader),
        0x3 => read_partition(reader),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown metadata record type",
        )),
    }
}

fn read_feature_level(reader: &mut Reader<'_>) -> io::Result<MetadataRecord> {
    let name = reader.read_compact_string()?;
    let feature_level = reader.read_i16()?;
    reader.skip_tagged_fields()?;

    Ok(MetadataRecord::FeatureLevel(FeatureLevelRecord {
        name,
        feature_level,
    }))
}

fn read_topic(reader: &mut Reader<'_>) -> io::Result<MetadataRecord> {
    let name = reader.read_compact_string()?;
    let topic_id = reader.read_uuid()?;
    reader.skip_tagged_fields()?;

    Ok(MetadataRecord::Topic(TopicRecord { name, topic_id }))
}

fn read_partition(reader: &mut Reader<'_>) -> io::Result<MetadataRecord> {
    let partition_id = reader.read_i32()?;
    let topic_id = reader.read_uuid()?;

    let replicas = reader.read_compact_i32_array()?;
    let isr = reader.read_compact_i32_array()?;
    let removing_replicas = reader.read_compact_i32_array()?;
    let adding_replicas = reader.read_compact_i32_array()?;

    let leader = reader.read_i32()?;
    let leader_epoch = reader.read_i32()?;
    let partition_epoch = reader.read_i32()?;

    let directories = reader.read_compact_uuid_array()?;

    reader.skip_tagged_fields()?;

    Ok(MetadataRecord::Partition(PartitionRecord {
        partition_id,
        topic_id,
        replicas,
        isr,
        removing_replicas,
        adding_replicas,
        leader,
        leader_epoch,
        partition_epoch,
        directories,
    }))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    #[test]
    fn replay_file_test() {
        let content: Vec<u8> = vec![
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //base-offset
            0x00, 0x00, 0x00, 0x4f, // batch-length
            0x00, 0x00, 0x00, 0x01, // partition leader epoch
            0x02, // magic byte
            0xb0, 0x69, 0x45, 0x7c, // CRC
            0x00, 0x00, // attribute
            0x00, 0x00, 0x00, 0x00, // last offset delta
            0x00, 0x00, 0x01, 0x91, 0xe0, 0x5a, 0xf8, 0x18, // base timestamp
            0x00, 0x00, 0x01, 0x91, 0xe0, 0x5a, 0xf8, 0x18, // max timestamp
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // producer id
            0xff, 0xff, // producer epoch
            0xff, 0xff, 0xff, 0xff, // base sequence,
            0x00, 0x00, 0x00, 0x01, // record length
            0x3a, // length
            0x00, // attribute
            0x00, // timestamp delta
            0x00, // offset delta
            0x01, // key length, zig-zag encoded: -1
            // null, no key
            0x2e, // value length
            0x01, // frame version
            0x0c, // type
            0x00, // version
            0x11, // name length
            0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x2e, 0x76, 0x65, 0x72, 0x73, 0x69,
            0x6f, 0x6e, // name
            0x00, 0x14, // feature level
            0x00, // tagged field count
            0x00, // header array count -> END OF BATCH
            // BATCH 2
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // base offset
            0x00, 0x00, 0x00, 0xe4, // batch length
            0x00, 0x00, 0x00, 0x01, // partition leader epoch
            0x02, // magic byte
            0x24, 0xdb, 0x12, 0xdd, // CRC,
            0x00, 0x00, // attribute
            0x00, 0x00, 0x00, 0x02, // last offset delta
            0x00, 0x00, 0x01, 0x91, 0xe0, 0x5b, 0x2d, 0x15, // base timestamp
            0x00, 0x00, 0x01, 0x91, 0xe0, 0x5b, 0x2d, 0x15, // max timestamp
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // producer id
            0xff, 0xff, // producer epoch
            0xff, 0xff, 0xff, 0xff, // base sequence,
            0x00, 0x00, 0x00, 0x03, // record length
            0x3c, // length
            0x00, // attribute,
            0x00, // timestamp delta,
            0x00, // offset delta
            0x01, // key length,
            // O byte, key is null
            0x30, // value length
            0x01, // frame version
            0x02, // type: Topic record
            0x00, // version
            0x04, // name length, decoded to 3
            0x73, 0x61, 0x7a, // topic name
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x91, // uuid: 16 bytes
            0x00, // tagged field count
            0x00, // header array count --> END OF RECORD 1
            // RECORD2
            0x90, 0x01, // length
            0x00, // attribute
            0x00, // timestamp delta
            0x02, // offset delta
            0x01, // key length
            // NULL key
            0x82, 0x01, // value length
            0x01, // frame version
            0x03, // type: partition
            0x01, // version
            0x00, 0x00, 0x00, 0x00, // partition id
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x91, // topic uuid: 16 bytes
            0x02, // length of replica array
            0x00, 0x00, 0x00, 0x01, // replica array
            0x02, // length of sync replica array
            0x00, 0x00, 0x00, 0x01, // sync replica array
            0x01, // length of removing replica array, 0 for compact encoding
            0x01, // length of adding replica array, 0
            0x00, 0x00, 0x00, 0x01, // leader
            0x00, 0x00, 0x00, 0x00, // leader epoch
            0x00, 0x00, 0x00, 0x00, // partition epoch
            0x02, // length of directories array
            0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x01, // uuid, 16 bytes
            0x00, // tagged field count
            0x00, // header array count // END OF RECORD2
            // RECORD3
            0x90, 0x01, // length
            0x00, // attribute
            0x00, // timestamp delta
            0x04, // offset delta
            0x01, // key length
            0x82, 0x01, // value length
            0x01, // frame version
            0x03, // type: partition
            0x01, // version
            0x00, 0x00, 0x00, 0x01, // partition id
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x91, // topic uuid: 16 bytes
            0x02, // length of replica array
            0x00, 0x00, 0x00, 0x01, // replica array
            0x02, // length of sync replica array
            0x00, 0x00, 0x00, 0x01, // sync replica array
            0x01, // length of removing replica array, 0 for compact encoding
            0x01, // length of adding replica array, 0
            0x00, 0x00, 0x00, 0x01, // leader
            0x00, 0x00, 0x00, 0x00, // leader epoch
            0x00, 0x00, 0x00, 0x00, // partition epoch
            0x02, // length of directories array
            0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x01, // uuid, 16 bytes
            0x00, // tagged field count
            0x00, // header array count // END OF RECORD3
        ];
        let metadata = replay(Cursor::new(content));
        assert!(metadata.is_ok());
    }
}
