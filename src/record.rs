use anyhow::{Ok, Result};
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_vec, Value};
use std::io::{Read, Write};

use crate::MAX_FILE_LINES;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Record {
    version: u8,
    pub id: u64,
    timestamp: u64,
    data: Value,
}

impl Record {
    pub fn new(id: u64, timestamp: u64, data: Value) -> Self {
        Self {
            version: 1,
            id,
            timestamp,
            data,
        }
    }

    pub fn get_file_segment(id: u64) -> u64 {
        (id - 1) / MAX_FILE_LINES as u64
    }

    pub fn size(&self) -> Result<u64> {
        let mut total = 0;

        total += 1; // version byte
        total += 8; // id bytes
        total += 8; // timestamp bytes
        total += 8; // datalen bytes
        total += to_vec(&self.data)?.len() as u64; // data bytes
        total += 1; // \n byte

        Ok(total)
    }

    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&[self.version])?;
        writer.write_all(&self.id.to_be_bytes())?;
        writer.write_all(&self.timestamp.to_be_bytes())?;
        let data_bytes = to_vec(&self.data)?;
        let data_len = data_bytes.len() as u64;
        writer.write_all(&data_len.to_be_bytes())?;
        writer.write_all(&data_bytes)?;
        writer.write_all(b"\n")?; // Write newline character
        Ok(())
    }

    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        let mut version_buf = [0; 1];
        reader.read_exact(&mut version_buf)?;
        let version = version_buf[0];

        let mut id_bytes = [0; 8];
        reader.read_exact(&mut id_bytes)?;
        let id = u64::from_be_bytes(id_bytes);

        let mut timestamp_bytes = [0; 8];
        reader.read_exact(&mut timestamp_bytes)?;
        let timestamp = u64::from_be_bytes(timestamp_bytes);

        let mut data_len_bytes = [0; 8];
        reader.read_exact(&mut data_len_bytes)?;
        let data_len = u64::from_be_bytes(data_len_bytes) as usize;

        let mut data_bytes = vec![0; data_len];
        reader.read_exact(&mut data_bytes)?;

        let mut newline_bytes = [0; 1];
        reader.read_exact(&mut newline_bytes)?;

        Ok(Record {
            version,
            id,
            timestamp,
            data: from_slice(&data_bytes)?,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_file_segment_from_plain_id() {
        assert_eq!(Record::get_file_segment(1), 0);
        assert_eq!(Record::get_file_segment(100), 0);
        assert_eq!(Record::get_file_segment(101), 1);
    }
}
