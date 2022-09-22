use memory_mapped::MemoryMapped;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    hash::{BuildHasherDefault, Hasher},
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
};

#[derive(Default)]
pub struct RdfTripleCompressor {
    translations: HashMap<u64, String, BuildHasherDefault<ahash::AHasher>>,
}

impl RdfTripleCompressor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn save_state<P: AsRef<Path>>(&mut self, path: P) -> std::io::Result<()> {
        let header_size = self.translations.len() * std::mem::size_of::<(u64, usize, usize)>();

        {
            let f = OpenOptions::new().write(true).create(true).open(&path)?;
            let mut bw = BufWriter::new(f);

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
        }

        {
            let mut hdr: MemoryMapped<[(u64, usize, usize)]> = unsafe {
                MemoryMapped::options()
                    .read(true)
                    .write(true)
                    .byte_offset(std::mem::size_of::<usize>())
                    .byte_len(header_size)
                    .open_shared_slice(path)?
                    .assume_init()
            };

            hdr.sort_unstable_by_key(|(hash, _, _)| *hash);
        }

        Ok(())
    }

    pub fn from_decompressor(frozen: super::decompressor::RdfTripleDecompressor) -> Self {
        let mut translations = HashMap::default();

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

    pub fn compress_rdf_triple_file<P: AsRef<Path>>(&mut self, path: P, dedup: bool) -> std::io::Result<()> {
        let out_path = path.as_ref().with_extension(super::COMPRESSED_TRIPLE_FILE_EXTENSION);

        {
            let mut bw = BufWriter::new(File::options().write(true).create_new(true).open(&out_path)?);

            let triples = BufReader::new(File::open(path)?).lines();

            for line in triples {
                let line = line?;

                if line.is_empty() || line.starts_with('#') {
                    continue;
                }

                let Some([subject, predicate, object]) = crate::rdf::split_rdf_triple(&line) else {
                    eprintln!("ignoring invalid rdf triple: {line:?}");
                    continue;
                };

                if subject.starts_with('_') || object.starts_with('_') {
                    // ignore triples with blank nodes
                    continue;
                }

                let [subject, predicate, object] = self.compress_rdf_triple_str([subject, predicate, object]);

                bw.write_all(&subject.to_ne_bytes())?;
                bw.write_all(&predicate.to_ne_bytes())?;
                bw.write_all(&object.to_ne_bytes())?;
            }
        }

        if dedup {
            let f = File::options().read(true).write(true).open(out_path)?;

            let n_uniq_triples = {
                // sort and deduplicate triples

                let mut mapped_slice: MemoryMapped<[[u64; 3]]> = unsafe {
                    MemoryMapped::options()
                        .read(true)
                        .write(true)
                        .open_shared_slice_from_file(&f)?
                        .assume_init()
                };

                mapped_slice.sort_unstable();
                let (uniq, _) = mapped_slice.partition_dedup();

                uniq.len()
            };

            f.set_len((n_uniq_triples * std::mem::size_of::<[u64; 3]>()) as u64)?;
        }

        Ok(())
    }
}
