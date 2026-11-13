use std::{io::Write, net::TcpStream};

use crate::{
    request::{Request, parse_request},
    response::{ApiKey, ApiVersionsResponse, Response, ResponseBody},
};

pub struct Handler;

impl Handler {
    pub fn handle(&self, mut stream: TcpStream) {
        loop {
            let request = match parse_request(&mut stream) {
                Ok(req) => req,
                Err(e) => {
                    eprintln!("Error: {e}");
                    break;
                }
            };
            let response = self.handle_request(&request);
            if let Err(e) = stream.write_all(&response.serialize()) {
                eprintln!("Failed to write response: {e}");
                break;
            }
        }
    }
    fn handle_request(&self, request: &Request) -> Response {
        let error_code = if (0..=4).contains(&request.api_version) {
            0i16
        } else {
            35i16
        };
        Response {
            correlation_id: request.correlation_id,
            body: ResponseBody::ApiVersions(ApiVersionsResponse {
                error_code,
                api_keys: vec![
                    ApiKey {
                        api_key: 18,
                        min_version: 0,
                        max_version: 4,
                    },
                    ApiKey {
                        api_key: 75,
                        min_version: 0,
                        max_version: 0,
                    },
                ],
                throttle_time_ms: 0,
            }),
        }
    }
}
