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
    key: Vec<u8>,
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

pub fn replay_file(path: &str) -> io::Result<Metadata> {
    let file = File::open(path)?;
    let mut file = BufReader::new(file);
    let mut metadata = Metadata::new();

    while let Some(batch) = read_batch(&mut file)? {
        for record in batch.records {
            metadata.apply(record)?;
        }
    }

    Ok(metadata)
}

fn read_batch(file: &mut BufReader<File>) -> io::Result<Option<RecordBatch>> {
    println!("====== start reading batch");
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

fn read_i64(file: &mut BufReader<File>) -> io::Result<i64> {
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf)?;
    Ok(i64::from_be_bytes(buf))
}

fn read_i32(file: &mut BufReader<File>) -> io::Result<i32> {
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

fn read_record(reader: &mut Reader<'_>) -> io::Result<Record> {
    println!("========= start reading record");
    let record_length = reader.read_varint()?;
    if record_length < 0 {
        return Err(io_invalid_data("negative record length"));
    }
    println!("=========== record length: {record_length}");
    let record_buf = reader.read_bytes(record_length as usize)?;
    let mut record_reader = Reader::new(record_buf);

    let attributes = record_reader.read_i8()?;
    println!("========= attriubte: {attributes}");
    let timestamp_delta = record_reader.read_varlong()?;
    println!("======== timestamp delta: {timestamp_delta}");
    let offset_delta = record_reader.read_varint()?;
    println!("========= offset_delta: {offset_delta}");
    let key_length = record_reader.read_varint()?;
    println!("========= key_length: {key_length}");

    let key = record_reader.read_bytes(key_length as usize)?.to_vec();
    println!("========= key: {:?}", key);

    let value_length = record_reader.read_varint()?;
    println!("========= value_length: {value_length}");

    let value_buf = record_reader.read_bytes(value_length as usize)?;
    println!("========= value_buf: {:?}", value_buf);

    let mut value_reader = Reader::new(value_buf);

    let value = read_metadata_record(&mut value_reader)?;
    println!("========= value done?");

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
    println!("======== start reading metadata record");
    let frame_version = reader.read_u8()?;
    if frame_version != 0 {
        return Err(io_invalid_data("unsupported frame version"));
    }
    let record_type = reader.read_u8()?;
    let _version = reader.read_u8()?;

    match record_type {
        0x0c => read_feature_level(reader),
        0x1 => read_topic(reader),
        0x2 => read_partition(reader),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown metadata record type",
        )),
    }
}

fn read_feature_level(reader: &mut Reader<'_>) -> io::Result<MetadataRecord> {
    println!("===== read feature level");
    let name = reader.read_compact_string()?;
    let feature_level = reader.read_i16()?;
    reader.skip_tagged_fields()?;

    Ok(MetadataRecord::FeatureLevel(FeatureLevelRecord {
        name,
        feature_level,
    }))
}

fn read_topic(reader: &mut Reader<'_>) -> io::Result<MetadataRecord> {
    println!("========= read topic");
    let name = reader.read_compact_string()?;
    let topic_id = reader.read_uuid()?;
    reader.skip_tagged_fields()?;

    Ok(MetadataRecord::Topic(TopicRecord { name, topic_id }))
}

fn read_partition(reader: &mut Reader<'_>) -> io::Result<MetadataRecord> {
    println!("========== read partition");
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
