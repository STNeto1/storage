use std::{
    fs::OpenOptions,
    io::{Read, Write},
    path::Path,
    time::UNIX_EPOCH,
};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_vec, Value};

fn main() -> Result<()> {
    if !Path::new("data").exists() {
        println!("Folder does not exist");
        std::fs::create_dir("data")?;
    }

    let recs = 1_000;

    for i in 1..=recs {
        let record = Record::new(
            i,
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_secs(),
            Value::Array(vec![
                Value::Null,
                Value::Number(1.into()),
                Value::String("hello".into()),
            ]),
        );

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(format!("data/records_{}.bin", Record::get_file_segment(i)))?;
        record.write_to(&mut file)?;
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Record {
    version: u8,
    id: u64,
    timestamp: u64,
    data: Value,
}

impl Record {
    fn new(id: u64, timestamp: u64, data: Value) -> Self {
        Self {
            version: 1,
            id,
            timestamp,
            data,
        }
    }

    fn get_file_segment(id: u64) -> u64 {
        id / 100
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
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

    fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
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
