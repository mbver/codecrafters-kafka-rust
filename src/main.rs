#![allow(unused_imports)]
use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

use crate::{
    handler::Handler,
    request::parse_request,
    response::{ApiKey, ApiVersionsResponse, Response, ResponseBody},
};
mod handler;
mod request;
mod response;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                thread::spawn(move || {
                    Handler.handle(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
