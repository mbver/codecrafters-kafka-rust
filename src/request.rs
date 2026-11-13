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
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf)?; // read msg_size

    stream.read_exact(&mut buf[..2])?;
    let api_key = i16::from_be_bytes(buf[..2].try_into().unwrap());

    stream.read_exact(&mut buf[..2])?;
    let api_version = i16::from_be_bytes(buf[..2].try_into().unwrap());

    stream.read_exact(&mut buf)?;
    let correlation_id = i32::from_be_bytes(buf);

    Ok(Request {
        api_key,
        api_version,
        correlation_id,
    })
}
