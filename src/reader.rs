use std::io;

use uuid::Uuid;

pub struct Reader<'a> {
    buf: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, offset: 0 }
    }
    pub(crate) fn read_bytes(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(n)
            .ok_or_else(|| io_invalid_data(&format!("offset overflow after adding: {n}")))?;

        let bytes = self
            .buf
            .get(self.offset..end)
            .ok_or(io::ErrorKind::UnexpectedEof)?;

        self.offset = end;
        Ok(bytes)
    }
    pub fn read_u8(&mut self) -> io::Result<u8> {
        Ok(self.read_bytes(1)?[0])
    }
    pub(crate) fn read_i8(&mut self) -> io::Result<i8> {
        Ok(self.read_u8()? as i8)
    }
    pub fn read_i16(&mut self) -> io::Result<i16> {
        let bytes = self.read_bytes(2)?;
        Ok(i16::from_be_bytes(bytes.try_into().unwrap()))
    }
    pub(crate) fn read_u32(&mut self) -> io::Result<u32> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
    }
    pub fn read_i32(&mut self) -> io::Result<i32> {
        let bytes = self.read_bytes(4)?;
        Ok(i32::from_be_bytes(bytes.try_into().unwrap()))
    }

    pub(crate) fn read_i64(&mut self) -> io::Result<i64> {
        let bytes = self.read_bytes(8)?;
        Ok(i64::from_be_bytes(bytes.try_into().unwrap()))
    }
    pub(crate) fn read_uvarint(&mut self) -> io::Result<usize> {
        Ok(self.read_uvarint_u32()? as usize)
    }
    fn read_uvarint_u32(&mut self) -> io::Result<u32> {
        let mut value = 0u32;

        for shift in (0..=28).step_by(7) {
            let byte = self.read_u8()?;
            let bits = (byte & 0x7f) as u32;

            if shift == 28 && bits > 0x0f {
                return Err(io_invalid_data("uvarint overflow"));
            }

            value |= bits << shift;

            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }

        Err(io_invalid_data("uvarint overflow"))
    }
    pub(crate) fn read_varint(&mut self) -> io::Result<i32> {
        let value = self.read_uvarint_u32()?;
        Ok((value >> 1) as i32 ^ -((value & 1) as i32))
    }
    fn read_uvarint_u64(&mut self) -> io::Result<u64> {
        let mut value = 0u64;

        for shift in (0..=63).step_by(7) {
            let byte = self.read_u8()?;
            let bits = (byte & 0x7f) as u64;

            if shift == 63 && bits > 1 {
                return Err(io_invalid_data("uvarlong overflow"));
            }

            value |= bits << shift;

            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }

        Err(io_invalid_data("uvarlong overflow"))
    }
    pub fn read_varlong(&mut self) -> io::Result<i64> {
        let value = self.read_uvarint_u64()?;
        Ok(((value >> 1) as i64) ^ (-((value & 1) as i64)))
    }

    pub(crate) fn read_uuid(&mut self) -> io::Result<Uuid> {
        let bytes: [u8; 16] = self.read_bytes(16)?.try_into().unwrap();
        Ok(Uuid::from_bytes(bytes))
    }
    pub(crate) fn read_compact_uuid_array(&mut self) -> io::Result<Vec<Uuid>> {
        let len = self.read_compact_array_len()?;
        let mut values = Vec::with_capacity(len);

        for _ in 0..len {
            values.push(self.read_uuid()?);
        }

        Ok(values)
    }
    pub fn read_compact_array_len(&mut self) -> io::Result<usize> {
        let len = self.read_uvarint()?;

        if len == 0 {
            return Err(io_invalid_data("null compact array"));
        }

        Ok(len - 1)
    }
    pub(crate) fn read_compact_i32_array(&mut self) -> io::Result<Vec<i32>> {
        let len = self.read_compact_array_len()?;
        let mut values = Vec::with_capacity(len);

        for _ in 0..len {
            values.push(self.read_i32()?);
        }

        Ok(values)
    }

    pub(crate) fn skip_tagged_fields(&mut self) -> io::Result<()> {
        let count = self.read_uvarint()?;

        for _ in 0..count {
            let _tag = self.read_uvarint()?;
            let size = self.read_uvarint()?;
            self.read_bytes(size)?;
        }

        Ok(())
    }
    pub fn skip_headers(&mut self) -> io::Result<()> {
        let count = self.read_varint()?;

        if count < 0 {
            return Err(io_invalid_data("negative header count"));
        }

        for _ in 0..count {
            let key_len = self.read_varint()?;
            if key_len < 0 {
                return Err(io_invalid_data("negative header key length"));
            }
            self.read_bytes(key_len as usize)?;

            let value_len = self.read_varint()?;
            if value_len >= 0 {
                self.read_bytes(value_len as usize)?;
            } else if value_len != -1 {
                return Err(io_invalid_data("invalid header value length"));
            }
        }

        Ok(())
    }
    pub fn read_compact_string(&mut self) -> io::Result<String> {
        let len = self.read_uvarint()?;
        if len == 0 {
            return Err(io_invalid_data("null compact string"));
        }
        let bytes = self.read_bytes(len - 1)?;
        let s = std::str::from_utf8(bytes)
            .map_err(|_| io_invalid_data("read_compact_string: invalid utf8"))?;

        Ok(s.to_owned())
    }
    pub fn read_nullable_string(&mut self) -> io::Result<Option<String>> {
        let len = self.read_i16()?;

        if len == -1 {
            return Ok(None);
        }

        if len < -1 {
            return Err(io_invalid_data("invalid nullable string length"));
        }

        let bytes = self.read_bytes(len as usize)?;
        let s = std::str::from_utf8(bytes)
            .map_err(|_| io_invalid_data("read_nullable_string: invalid utf8"))?;

        Ok(Some(s.to_owned()))
    }
    pub fn read_tag_buffer(&mut self) -> io::Result<()> {
        if self.read_u8()? != 0 {
            return Err(io_invalid_data("expect tag buffer to be 0"));
        }
        Ok(())
    }
    pub fn is_empty(&self) -> bool {
        self.offset == self.buf.len()
    }
}

pub fn io_invalid_data(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}
