```rs
struct Topic {
    name: String,
    id: Uuid,
    partitions: Vec<Partition>,
}

struct Partition {
    id: i32,
    leader: i32,
    leader_epoch: i32,
    replicas: Vec<i32>,
    isr: Vec<i32>,
}
```

batch format:

- Header
  - Base Offset
  - Batch Length
  - Partition Leader Epoch
  - Magic Byte // skip for now
  - CRC // skip crc check for now
  - Attributes
  - Last Offset Delta
  - Base Timestamp
  - Max Timestamp
  - Producer ID
  - Producer Epoch
  - Base Sequence
  - Records Length // read record length and loop
- Records
  - record#1
  - record#2
  - record#3

- record format
  - Length
  - Attributes
  - Timestamp Delta
  - Offset Delta
  - Key Length
  - Key
  - Value Length
  - Value (Feature Level Record) -> focus here
  - Headers Array Count

- value format: feature
  - Frame Version
  - Type
  - Version
  - Name length
  - Name
  - Feature Level
  - Tagged Fields Count

- value format: topic
  - Frame Version
  - Type
  - Version
  - Name length
  - Topic Name
  - Topic UUID
  - Tagged Fields Count

- value format: partition
  - Frame Version
  - Type
  - Version
  - Partition ID
  - Topic UUID
  - Length of replica array
  - Replica Array
  - Length of In Sync Replica array
  - In Sync Replica Array
  - Length of Removing Replicas array
  - Length of Adding Replicas array
  - Leader
  - Leader Epoch
  - Partition Epoch
  - Length of Directories array
  - Directories Array
  - Tagged Fields Count
