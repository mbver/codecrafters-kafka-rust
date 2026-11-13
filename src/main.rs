#![allow(unused_imports)]
use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // TODO: Uncomment the code below to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let request = match parse_request(&mut stream) {
                    Ok(request) => request,
                    Err(e) => {
                        eprintln!("Error: {e}");
                        continue;
                    }
                };
                let error_code = if (0..=4).contains(&request.api_version) {
                    0i16
                } else {
                    35i16
                };
                let response = Response {
                    correlation_id: request.correlation_id,
                    body: ResponseBody::ApiVersions(ApiVersionsResponse {
                        error_code,
                        api_keys: vec![ApiKey {
                            api_key: 18,
                            min_version: 0,
                            max_version: 4,
                        }],
                        throttle_time_ms: 0,
                    }),
                };
                stream
                    .write_all(&response.serialize())
                    .expect("fail to write to stream");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

struct Request {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
}

fn parse_request(stream: &mut TcpStream) -> io::Result<Request> {
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

struct Response {
    pub correlation_id: i32,
    pub body: ResponseBody,
}

pub enum ResponseBody {
    ApiVersions(ApiVersionsResponse),
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
        match &self.body {
            ResponseBody::ApiVersions(body) => body.serialize_into(&mut buf),
        }
        let msg_size = buf.len() - 4;
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
