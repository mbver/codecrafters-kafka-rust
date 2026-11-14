#![allow(unused_imports)]
use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    process,
    sync::{Arc, Mutex},
    thread,
};

use crate::{
    handler::Handler,
    metadata::replay_file,
    request::parse_request,
    response::{ApiKey, ApiVersionsResponse, Response, ResponseBody},
};
mod handler;
mod metadata;
mod reader;
mod request;
mod response;

fn main() {
    let metadata =
        match replay_file("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
        {
            Ok(m) => Arc::new(m),
            Err(e) => {
                eprintln!("Error: {e}");
                process::exit(1);
            }
        };
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                let metadata = metadata.clone();
                thread::spawn(move || {
                    Handler::new(metadata).handle(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
