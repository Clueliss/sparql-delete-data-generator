use crate::MemoryMapped;
use std::{fs::File, io::Read, path::Path};

pub struct FrozenRdfTripleCompressor {
    pub(super) header: MemoryMapped<[(u64, usize, usize)]>,
    pub(super) data_segment: MemoryMapped<[u8]>,
}

impl FrozenRdfTripleCompressor {
    fn search_header(&self, hash: u64) -> Option<&(u64, usize, usize)> {
        let ix = self.header.binary_search_by_key(&hash, |(h, _, _)| *h).ok()?;
        Some(&self.header[ix])
    }

    pub fn load_frozen<P: AsRef<Path>>(path: P) -> std::io::Result<FrozenRdfTripleCompressor> {
        let header_size = {
            let mut f = File::open(path.as_ref())?;

            let mut header_size_buf = [0; std::mem::size_of::<usize>()];
            f.read_exact(&mut header_size_buf)?;

            usize::from_ne_bytes(header_size_buf)
        };

        let header = MemoryMapped::options()
            .read(true)
            .byte_offset(std::mem::size_of::<usize>())
            .byte_len(header_size)
            .open(path.as_ref())?;

        let data_segment = MemoryMapped::options()
            .read(true)
            .byte_offset(std::mem::size_of::<usize>() + header_size)
            .open(path.as_ref())?;

        Ok(Self { header, data_segment })
    }

    pub fn decompress_rdf_triple(&self, [subject, predicate, object]: &[u64; 3]) -> Option<[&str; 3]> {
        let &(_, s_start, s_end) = self.search_header(*subject)?;
        let &(_, p_start, p_end) = self.search_header(*predicate)?;
        let &(_, o_start, o_end) = self.search_header(*object)?;

        Some(unsafe {
            [
                std::str::from_utf8_unchecked(&self.data_segment[s_start..s_end]),
                std::str::from_utf8_unchecked(&self.data_segment[p_start..p_end]),
                std::str::from_utf8_unchecked(&self.data_segment[o_start..o_end]),
            ]
        })
    }
}
