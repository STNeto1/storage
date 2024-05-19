use std::fs;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::{MAX_FILE_LINES, META_FILE};

#[derive(Debug, Serialize, Deserialize)]
pub struct Meta {
    jumps: Vec<Vec<u64>>,
}

impl Meta {
    pub fn new() -> Self {
        let jumps = vec![vec![0; MAX_FILE_LINES]; 0];

        Self { jumps }
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
        let id = id - 1;

        let page: usize = (id / MAX_FILE_LINES as u64)
            .try_into()
            .context("Failed to divide?")?;

        match self.jumps.get(page) {
            Some(_) => (),
            None => self.jumps.insert(page, vec![0; MAX_FILE_LINES]),
        }

        match id % MAX_FILE_LINES as u64 {
            0 => {
                self.jumps[page][0] = 0;
                // self.jumps[page][1] = size;
            }
            val => {
                self.jumps[page][val as usize] = self.jumps[page][(val - 1) as usize] + size;
            }
        }

        Ok(())
    }

    pub fn get_segment_offset(&self, id: &u64) -> Result<u64> {
        let id = id - 1;

        let page_idx: usize = (id / MAX_FILE_LINES as u64)
            .try_into()
            .context("Failed to divide?")?;

        // println!("{id} - {page_idx}");

        let relative_id: u64 = id % MAX_FILE_LINES as u64;

        match self.jumps.get(page_idx) {
            Some(cell) => {
                if let Some(offset) = cell.get((relative_id) as usize) {
                    let offset = offset.to_owned();

                    // value isn't initialized but is truing to access it
                    if offset == 0 && relative_id != 0 {
                        anyhow::bail!("Invalid offset");
                    }

                    return Ok(offset);
                } else {
                    anyhow::bail!("Value should have been found");
                }
            }
            None => anyhow::bail!("Page not found".to_string()),
        }
    }
}
