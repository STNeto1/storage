use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{self, BufRead, BufReader, Read, Seek, Write},
    ops::Add,
    path::Path,
    time::UNIX_EPOCH,
    u64,
};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_vec, Value};

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
        meta.add_to_collection(record.id, record.size()?.into());

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(format!("data/records_{}.bin", Record::get_file_segment(i)))?;
        record.write_to(&mut file)?;
    }

    let id = 82;
    let file_path = format!("data/records_{}.bin", Record::get_file_segment(id));
    let mut file = OpenOptions::new().read(true).open(file_path)?;

    file.seek(io::SeekFrom::Start(meta.get_from_collection(&id)))?;
    match Record::read_from(&mut file) {
        Ok(rec) => {
            println!("record found => {:?}", rec,);
        }
        Err(err) => {
            println!("error reading contents: {err}");
        }
    }

    // loop {
    //     match Record::read_from(&mut file) {
    //         Ok(rec) => {
    //             if rec.id == id {
    //                 println!(
    //                     "record found => {:?} | Offset => {:?}",
    //                     rec,
    //                     meta.get_from_collection(&rec.id)
    //                 );
    //                 break;
    //             }
    //         }
    //         Err(err) => {
    //             println!("error reading contents: {err}");
    //             break;
    //         }
    //     }
    // }

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
    jumps: HashMap<u64, u64>,
}

impl Meta {
    fn new() -> Self {
        let mut jumps: HashMap<u64, u64> = HashMap::new();
        jumps.insert(1, 0);

        Self { jumps }
    }

    fn add_to_collection(&mut self, id: u64, size: u64) {
        let val = self.jumps.get(&id).unwrap_or(&0).to_owned();

        self.jumps.insert(id + 1, val + size);
    }

    fn get_from_collection(&self, id: &u64) -> u64 {
        return self.jumps.get(&id).unwrap_or(&0).to_owned();
    }
}
