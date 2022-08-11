pub mod frozen;

use crate::MemoryMapped;
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    hash::Hasher,
    io::{BufRead, BufReader, BufWriter, ErrorKind, Write},
    path::Path,
};

#[derive(Default)]
pub struct RdfTripleCompressor {
    translations: BTreeMap<u64, String>,
}

impl RdfTripleCompressor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn freeze<P: AsRef<Path>>(&mut self, path: P) -> std::io::Result<()> {
        let f = OpenOptions::new().write(true).create_new(true).open(path)?;

        let mut bw = BufWriter::new(f);

        let header_size = self.translations.len() * std::mem::size_of::<(u64, usize, usize)>();
        bw.write_all(&header_size.to_ne_bytes())?;

        let mut data_segment_off: usize = 0;
        for (hash, rdf_str) in &self.translations {
            bw.write_all(&hash.to_ne_bytes())?;
            bw.write_all(&data_segment_off.to_ne_bytes())?;

            data_segment_off += rdf_str.as_bytes().len();
            bw.write_all(&data_segment_off.to_ne_bytes())?;
        }

        for rdf_str in self.translations.values() {
            bw.write_all(rdf_str.as_bytes())?;
        }

        Ok(())
    }

    pub fn from_frozen(frozen: frozen::FrozenRdfTripleCompressor) -> Self {
        let mut translations = BTreeMap::new();

        for (hash, s_beg, s_end) in frozen.header {
            let rdf_data = frozen.data_segment[s_beg..s_end].to_owned();

            translations.insert(hash, unsafe { String::from_utf8_unchecked(rdf_data) });
        }

        Self { translations }
    }

    pub fn compress_rdf_triple_str(&mut self, [subject, predicate, object]: [&str; 3]) -> [u64; 3] {
        let hash = |s| {
            let mut hasher = ahash::AHasher::default();
            hasher.write_str(s);
            hasher.finish()
        };

        let subject_hash = hash(subject);
        let predicate_hash = hash(predicate);
        let object_hash = hash(object);

        self.translations.insert(subject_hash, subject.to_owned());
        self.translations.insert(predicate_hash, predicate.to_owned());
        self.translations.insert(object_hash, object.to_owned());

        [subject_hash, predicate_hash, object_hash]
    }

    pub fn decompress_rdf_triple(&self, [subject, predicate, object]: [u64; 3]) -> Option<[&str; 3]> {
        Some([
            self.translations.get(&subject)?,
            self.translations.get(&predicate)?,
            self.translations.get(&object)?,
        ])
    }

    pub fn compress_rdf_triple_file<P: AsRef<Path>>(&mut self, path: P) -> std::io::Result<()> {
        {
            let mut bw = BufWriter::new(
                File::options()
                    .write(true)
                    .create_new(true)
                    .open(path.as_ref().with_extension("compressed"))?,
            );

            let triples = BufReader::new(File::open(path.as_ref())?).lines();

            for line in triples {
                let line = line?;

                if line.starts_with('#') {
                    continue;
                }

                let triple = super::split_rdf_triple(&line)
                    .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidData, "invalid rdf triple found"))?;

                let [subject, predicate, object] = self.compress_rdf_triple_str(triple);

                bw.write_all(&subject.to_ne_bytes())?;
                bw.write_all(&predicate.to_ne_bytes())?;
                bw.write_all(&object.to_ne_bytes())?;
            }
        }

        let mut mapped_slice: MemoryMapped<[[u64; 3]]> = unsafe {
            MemoryMapped::options()
                .read(true)
                .write(true)
                .open_shared(path.as_ref().with_extension("compressed"))
        }?;

        mapped_slice.sort_unstable();

        Ok(())
    }
}
