use std::{fs, sync::Mutex, usize};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::{record, META_FILE};

#[derive(Debug, Serialize, Deserialize)]
pub struct Meta {
    sequence: Vec<u64>,
    jumps: Vec<Vec<Option<u64>>>,
}

impl Meta {
    pub fn new() -> Self {
        let jumps = vec![];

        Self {
            sequence: vec![],
            jumps,
        }
    }

    pub fn read_from_file() -> Result<Self> {
        let raw_data = fs::read_to_string(META_FILE).context("to read data/meta.json file")?;
        let meta: Meta = serde_json::from_str(&raw_data).context("to read raw_data to Meta")?;

        Ok(meta)
    }

    pub fn write_to_file(&self) -> Result<()> {
        let json_data = serde_json::to_vec_pretty(&self)?;
        fs::write(META_FILE, json_data).context("failed to write plain json meta.json")?;

        Ok(())
    }

    pub fn add_to_collection(&mut self, id: u64, size: u64) -> Result<()> {
        let segment = record::Record::get_file_segment(id);

        match self.jumps.get(segment as usize) {
            Some(_) => (),
            None => self.jumps.insert(segment as usize, vec![]),
        }

        match self.sequence.get(segment as usize) {
            Some(_) => (),
            None => self.sequence.insert(segment as usize, 0),
        }

        let segment_sequence = self
            .sequence
            .get(segment as usize)
            .context("should be at least defined above")?;
        let segment_page = self
            .jumps
            .get_mut(segment as usize)
            .context("should be at least defined above")?;

        match id {
            1 => {
                segment_page.insert(0, Some(0));
                segment_page.insert(1, Some(size));
            }
            val => match segment_page.get((val - 1) as usize) {
                Some(back_value) => match back_value {
                    Some(inner_value) => {
                        segment_page.insert(val as usize, Some(inner_value + size));
                    }
                    _ => unimplemented!("Back value exists but is None"),
                },
                None => {
                    unreachable!("Back value doesn't exist");
                }
            },
        }

        self.sequence[segment as usize] = segment_sequence + 1;

        Ok(())
    }

    pub fn get_segment_offset(&self, id: &u64) -> Result<u64> {
        let segment = record::Record::get_file_segment(id.to_owned());

        match self.jumps.get(segment as usize) {
            Some(_) => (),
            None => anyhow::bail!("Page does not exist"),
        }

        match self.sequence.get(segment as usize) {
            Some(seq) => {
                // Value should exist before being set-up
                if id.to_owned() > seq.to_owned() {
                    anyhow::bail!("Value doesn't exist yet");
                }
            }
            None => anyhow::bail!("Sequence does not exist"),
        }

        let segment_page = self
            .jumps
            .get(segment as usize)
            .context("should be at least defined above")?;

        match segment_page.get((id - 1) as usize) {
            Some(cell) => match cell {
                Some(offset) => Ok(offset.to_owned()),
                None => anyhow::bail!("Offset was set yet?"),
            },
            None => anyhow::bail!("Record not found".to_string()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::record::Record;
    use anyhow::Result;

    #[test]
    fn test_file_segment_from_plain_id() {
        assert_eq!(Record::get_file_segment(1), 0);
        assert_eq!(Record::get_file_segment(100), 0);
        assert_eq!(Record::get_file_segment(101), 1);
    }

    #[test]
    fn test_adding_to_collection() -> Result<()> {
        let mut meta = Meta::new();

        meta.add_to_collection(1, 10)
            .context("should add without issues")?;
        meta.add_to_collection(2, 15)
            .context("should add without issues")?;
        meta.add_to_collection(3, 20)
            .context("should add without issues")?;

        // [[Some(0), Some(10), Some(25), Some(45)]]
        //   ^1       ^2        ^3        ^4

        // id => 1
        assert_eq!(
            meta.jumps
                .get(0)
                .context("page should exist")?
                .get(0)
                .context("should exist")?
                .to_owned(),
            Some(0)
        );

        // id => 2
        assert_eq!(
            meta.jumps
                .get(0)
                .context("page should exist")?
                .get(1)
                .context("should exist")?
                .to_owned(),
            Some(10)
        );

        // id => 3
        assert_eq!(
            meta.jumps
                .get(0)
                .context("page should exist")?
                .get(2)
                .context("should exist")?
                .to_owned(),
            Some(25)
        );

        // id => 4 (doesn't exist yet?)
        assert_eq!(
            meta.jumps
                .get(0)
                .context("page should exist")?
                .get(3)
                .context("should exist")?
                .to_owned(),
            Some(45)
        );

        Ok(())
    }

    #[test]
    fn test_fetching_to_collection() -> Result<()> {
        let mut meta = Meta::new();

        meta.add_to_collection(1, 10)
            .context("should add without issues")?;
        meta.add_to_collection(2, 15)
            .context("should add without issues")?;
        meta.add_to_collection(3, 20)
            .context("should add without issues")?;

        // [[Some(0), Some(10), Some(25), Some(45)]]
        //   ^1       ^2        ^3        ^4

        assert_eq!(
            meta.get_segment_offset(&1)
                .context("should exist")?
                .to_owned(),
            0
        );

        assert_eq!(
            meta.get_segment_offset(&2)
                .context("should exist")?
                .to_owned(),
            10
        );

        assert_eq!(
            meta.get_segment_offset(&3)
                .context("should exist")?
                .to_owned(),
            25
        );

        assert!(meta.get_segment_offset(&4).is_err());

        Ok(())
    }
}
