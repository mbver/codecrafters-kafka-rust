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
                let mut resp = Vec::<u8>::new();
                resp.extend_from_slice(&0i32.to_be_bytes());
                resp.extend_from_slice(&request.correlation_id.to_be_bytes());
                resp.extend_from_slice(&error_code.to_be_bytes());
                stream.write_all(&resp).expect("fail to write to stream");
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
