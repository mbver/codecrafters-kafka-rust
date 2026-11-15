use std::{
    io::{self, Read},
    net::TcpStream,
};

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

pub struct FetchRequest;

pub enum RequestBody {
    ApiVersions,
    DescribeTopicPartitions(DescribeTopicPartitionsRequest),
    Fetch(FetchRequest),
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

fn parse_fetch(_reader: &mut Reader) -> io::Result<FetchRequest> {
    Ok(FetchRequest)
}
