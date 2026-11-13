use std::{
    io::{self, Read},
    net::TcpStream,
};

pub struct Request {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub body: RequestBody,
}

pub struct DescribeTopicPartitionsRequest {
    pub topic_name: String,
}

pub enum RequestBody {
    ApiVersions,
    DescribeTopicPartitions(DescribeTopicPartitionsRequest),
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
    let body = match api_key {
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
    let array_len = reader.read_uvarint()?;

    if array_len != 2 {
        return Err(io_invalid_data(&format!(
            "expect exactly 1 topic in the array, got {}, remaining: {:?}",
            array_len - 1,
            reader.buf,
        )));
    }

    let topic_name = reader.read_compact_string()?;
    reader.read_tag_buffer()?; // topic tag buffer
    reader.read_tag_buffer()?; // request tag buffer

    Ok(DescribeTopicPartitionsRequest { topic_name })
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
    fn read_u8(&mut self) -> io::Result<u8> {
        Ok(self.read_bytes(1)?[0])
    }

    fn read_i16(&mut self) -> io::Result<i16> {
        let bytes = self.read_bytes(2)?;
        Ok(i16::from_be_bytes(bytes.try_into().unwrap()))
    }
    fn read_i32(&mut self) -> io::Result<i32> {
        let bytes = self.read_bytes(4)?;
        Ok(i32::from_be_bytes(bytes.try_into().unwrap()))
    }
    fn read_uvarint(&mut self) -> io::Result<usize> {
        let mut value = 0usize;
        let mut shift = 0;
        loop {
            let byte = self.read_u8()?;
            value |= ((byte & 0x7f) as usize) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
            if shift >= usize::BITS {
                return Err(io_invalid_data("uvarint overflow"));
            }
        }
    }
    fn read_compact_string(&mut self) -> io::Result<String> {
        let len = self.read_uvarint()?;
        if len == 0 {
            return Err(io_invalid_data("null compact string"));
        }
        let bytes = self.read_bytes(len - 1)?;
        let s = std::str::from_utf8(bytes)
            .map_err(|_| io_invalid_data("read_compact_string: invalid utf8"))?;

        Ok(s.to_owned())
    }
    fn read_tag_buffer(&mut self) -> io::Result<()> {
        if self.read_u8()? != 0 {
            return Err(io_invalid_data("expect tag buffer to be 0"));
        }
        Ok(())
    }
}

fn io_invalid_data(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}
