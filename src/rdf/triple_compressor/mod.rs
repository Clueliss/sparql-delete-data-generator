pub mod compressor;
pub mod decompressor;

use crate::MemoryMapped;
use std::{ops::Deref, path::Path};

pub const COMPRESSOR_STATE_FILE_EXTENSION: &str = "compressor_state";
pub const COMPRESSED_TRIPLE_FILE_EXTENSION: &str = "compressed_nt";
pub const UNCOMPRESSED_TRIPLE_FILE_EXTENSION: &str = "nt";

pub struct CompressedRdfTriples(MemoryMapped<[[u64; 3]]>);

impl CompressedRdfTriples {
    pub unsafe fn load<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        Ok(CompressedRdfTriples(MemoryMapped::open_slice(path)?.assume_init()))
    }

    pub fn contains(&self, triple: &[u64; 3]) -> bool {
        self.0.binary_search(triple).is_ok()
    }

    pub fn into_inner(self) -> MemoryMapped<[[u64; 3]]> {
        self.0
    }
}

impl Deref for CompressedRdfTriples {
    type Target = MemoryMapped<[[u64; 3]]>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
