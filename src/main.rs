use std::{
    fs::{self, OpenOptions},
    io::{self, Seek},
    path::Path,
    time::UNIX_EPOCH,
};

use anyhow::{Context, Ok, Result};
use serde_json::Value;

const MAX_FILE_LINES: usize = 100;
const META_FILE: &'static str = "data/meta.json";

mod meta;
mod record;

fn main() -> Result<()> {
    check_base_line()?;

    let mut meta = meta::Meta::read_from_file()?;
    // let mut meta = meta::Meta::new();

    let recs = 201;

    for i in 1..=recs {
        let row = record::Record::new(
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
        meta.add_to_collection(row.id, row.size()?.into())?;

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(format!(
                "data/records_{}.bin",
                record::Record::get_file_segment(i)
            ))?;
        row.write_to(&mut file)?;

        // match (i - 1) % MAX_FILE_LINES as u64 {
        //     val => println!("written to {i} {}", val),
        // }
    }

    meta.write_to_file()?;

    for id in 1..=recs {
        let file_path = format!("data/records_{}.bin", record::Record::get_file_segment(id));

        let mut file = OpenOptions::new().read(true).open(file_path.to_owned())?;
        file.seek(io::SeekFrom::Start(meta.get_segment_offset(&id)?))?;
        let rec = record::Record::read_from(&mut file)?;
        println!("looking for {id} | found => {:?}", rec);
    }

    Ok(())
}

fn check_base_line() -> Result<()> {
    if !Path::new("data").exists() {
        std::fs::create_dir("data")?;
    }

    if !Path::new(META_FILE).exists() {
        let meta = meta::Meta::new();
        let json_data = serde_json::to_vec(&meta)?;
        fs::write(META_FILE, json_data).context("failed to write plain json meta.json")?;
    }

    Ok(())
}
