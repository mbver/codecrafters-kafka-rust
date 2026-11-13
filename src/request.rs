use std::{
    io::{self, Read},
    net::TcpStream,
};

pub struct Request {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
}

pub fn parse_request(stream: &mut TcpStream) -> io::Result<Request> {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf)?; // read msg_size
    let msg_size = i32::from_be_bytes(size_buf) as usize;

    let mut buf = vec![0u8; msg_size];
    stream.read_exact(&mut buf)?;

    let mut offset = 0;
    let api_key = i16::from_be_bytes(buf[offset..offset + 2].try_into().unwrap());
    offset += 2;
    let api_version = i16::from_be_bytes(buf[offset..offset + 2].try_into().unwrap());
    offset += 2;
    let correlation_id = i32::from_be_bytes(buf[offset..offset + 4].try_into().unwrap());
    offset += 4;

    Ok(Request {
        api_key,
        api_version,
        correlation_id,
    })
}
