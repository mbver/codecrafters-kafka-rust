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

    let mut reader = Reader::new(&buf);

    let api_key = reader.read_i16()?;
    let api_version = reader.read_i16()?;
    let correlation_id = reader.read_i32()?;
    Ok(Request {
        api_key,
        api_version,
        correlation_id,
    })
}

struct Reader<'a> {
    buf: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, offset: 0 }
    }
    fn read_bytes(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let end = self.offset + n;
        let bytes = self
            .buf
            .get(self.offset..end)
            .ok_or(io::ErrorKind::UnexpectedEof)?;
        self.offset = end;
        Ok(bytes)
    }
    fn read_i16(&mut self) -> io::Result<i16> {
        let bytes = self.read_bytes(2)?;
        Ok(i16::from_be_bytes(bytes.try_into().unwrap()))
    }
    fn read_i32(&mut self) -> io::Result<i32> {
        let bytes = self.read_bytes(4)?;
        Ok(i32::from_be_bytes(bytes.try_into().unwrap()))
    }
}
