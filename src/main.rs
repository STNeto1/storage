use std::{
    fs::OpenOptions,
    io::{self, Read, Seek, Write},
    path::Path,
    time::UNIX_EPOCH,
};

use anyhow::{Context, Ok, Result};
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_vec, Value};

const MAX_FILE_LINES: usize = 100;

fn main() -> Result<()> {
    if !Path::new("data").exists() {
        println!("Folder does not exist");
        std::fs::create_dir("data")?;
    }

    let mut meta = Meta::new();

    let recs = 1_00;

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
        meta.add_to_collection(record.id, record.size()?.into())?;

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(format!("data/records_{}.bin", Record::get_file_segment(i)))?;
        record.write_to(&mut file)?;
    }

    let id = 58;
    let file_path = format!("data/records_{}.bin", Record::get_file_segment(id));
    let mut file = OpenOptions::new().read(true).open(file_path.to_owned())?;
    file.seek(io::SeekFrom::Start(meta.get_segment_offset(&id)?))?;
    let rec = Record::read_from(&mut file)?;
    println!("looking for {id} | record found => {:?}", rec);

    // let mut file = OpenOptions::new().append(true).open(file_path)?;
    // let record = Record::new(
    //     id,
    //     std::time::SystemTime::now()
    //         .duration_since(UNIX_EPOCH)?
    //         .as_secs(),
    //     Value::Array(vec![
    //         Value::String("hello".into()),
    //         Value::Null,
    //         Value::Number(10.into()),
    //     ]),
    // );
    // file.seek(io::SeekFrom::Start(meta.get_segment_offset(&id)?))?;

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
        id / MAX_FILE_LINES as u64
    }

    fn size(&self) -> Result<u64> {
        let mut total = 0;

        total += 1; // version byte
        total += 8; // id bytes
        total += 8; // timestamp bytes
        total += 8; // datalen bytes
        total += to_vec(&self.data)?.len() as u64; // data bytes
        total += 1; // \n byte

        Ok(total)
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

#[derive(Debug)]
struct Meta {
    jumps: Vec<Vec<u64>>,
}

impl Meta {
    fn new() -> Self {
        let jumps = vec![vec![0; MAX_FILE_LINES]; 0];

        Self { jumps }
    }

    fn add_to_collection(&mut self, id: u64, size: u64) -> Result<()> {
        let id = id - 1;

        let outer_ref: usize = (id / MAX_FILE_LINES as u64)
            .try_into()
            .context("Failed to divide?")?;

        match self.jumps.get(outer_ref) {
            Some(_) => (),
            None => self.jumps.insert(outer_ref, vec![0; MAX_FILE_LINES]),
        }

        match id % MAX_FILE_LINES as u64 {
            0 => {
                self.jumps[outer_ref][0] = 0;
                self.jumps[outer_ref][1] = size;
            }
            val => {
                self.jumps[outer_ref][val as usize] =
                    self.jumps[outer_ref][(val - 1) as usize] + size;
            }
        }

        Ok(())
    }

    fn get_segment_offset(&self, id: &u64) -> Result<u64> {
        // let id = id - 1;

        let outer_ref: usize = (id / MAX_FILE_LINES as u64)
            .try_into()
            .context("Failed to divide?")?;

        let relative_id = id % MAX_FILE_LINES as u64;

        match self.jumps.get(outer_ref) {
            Some(cell) => {
                if let Some(offset) = cell.get(relative_id as usize) {
                    let offset = offset.to_owned();

                    match (offset, relative_id) {
                        (0, 0) => return Ok(offset), // Ok(0) basically
                        (0, _) => anyhow::bail!("Invalid offset"),
                        _ => return Ok(offset),
                    }
                } else {
                    anyhow::bail!("Value should have been found");
                }
            }
            None => anyhow::bail!("Page not found".to_string()),
        }
    }
}
