use super::TripleElementId;
use crate::rdf::triple_compressor::TripleId;
use rio_api::{
    model::{Subject, Term, Triple},
    parser::TriplesParser,
};
use std::{
    collections::{BTreeMap, HashSet},
    fs::{File, OpenOptions},
    hash::{BuildHasher, BuildHasherDefault, Hash, Hasher},
    io::{BufReader, BufWriter, Write},
    path::Path,
};

fn hash_single<T: Hash, H: BuildHasher>(to_hash: T, build_hasher: H) -> u64 {
    let mut hasher = build_hasher.build_hasher();
    to_hash.hash(&mut hasher);
    hasher.finish()
}

#[derive(Default)]
pub struct RdfTripleCompressor {
    translations: BTreeMap<TripleElementId, String>,
    dedup: HashSet<TripleId, BuildHasherDefault<ahash::AHasher>>,
}

impl RdfTripleCompressor {
    fn found_new_triple(&mut self, triple: [TripleElementId; 3]) -> bool {
        let hash = hash_single(triple, BuildHasherDefault::<ahash::AHasher>::default());
        self.dedup.insert(hash)
    }
}

impl RdfTripleCompressor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn save_state<P: AsRef<Path>>(&mut self, path: P) -> std::io::Result<()> {
        let header_size = self.translations.len() * std::mem::size_of::<(TripleElementId, usize, usize)>();

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

        Ok(())
    }

    pub fn from_decompressor(frozen: super::decompressor::RdfTripleDecompressor) -> Self {
        let mut translations = BTreeMap::default();

        for (hash, s_beg, s_end) in frozen.header {
            let rdf_data = frozen.data_segment[s_beg..s_end].to_owned();

            translations.insert(hash, unsafe { String::from_utf8_unchecked(rdf_data) });
        }

        Self { translations, dedup: HashSet::default() }
    }

    pub fn compress_rdf_triple(&mut self, triple: Triple) -> [TripleElementId; 3] {
        type BuildHasher = BuildHasherDefault<ahash::AHasher>;

        let subject_hash = hash_single(triple.subject, BuildHasher::default());
        let predicate_hash = hash_single(triple.predicate, BuildHasher::default());
        let object_hash = hash_single(triple.object, BuildHasher::default());

        self.translations
            .entry(subject_hash)
            .or_insert_with(|| triple.subject.to_string());
        self.translations
            .entry(predicate_hash)
            .or_insert_with(|| triple.predicate.to_string());
        self.translations
            .entry(object_hash)
            .or_insert_with(|| triple.object.to_string());

        [subject_hash, predicate_hash, object_hash]
    }

    pub fn compress_rdf_triple_file<P: AsRef<Path>>(&mut self, path: P, dedup: bool) -> std::io::Result<()> {
        let out_path = path.as_ref().with_extension(super::COMPRESSED_TRIPLE_FILE_EXTENSION);

        let mut bw = BufWriter::new(File::options().write(true).create_new(true).open(&out_path)?);
        let mut triples = rio_turtle::NTriplesParser::new(BufReader::new(File::open(path)?));

        let (writer_res, reader_res) = std::thread::scope(move |s| {
            let (tx, rx) = std::sync::mpsc::channel::<[TripleElementId; 3]>();

            let writer = s.spawn(move || -> std::io::Result<()> {
                while let Ok([s, p, o]) = rx.recv() {
                    bw.write_all(&s.to_ne_bytes())?;
                    bw.write_all(&p.to_ne_bytes())?;
                    bw.write_all(&o.to_ne_bytes())?;
                }

                Ok(())
            });

            let reader = s.spawn(move || -> std::io::Result<()> {
                while !triples.is_end() {
                    let res: Result<(), std::io::Error> = triples.parse_step(&mut |triple| {
                        let subject@Subject::NamedNode(_) = triple.subject else {
                            return Ok(());
                        };

                        let predicate = triple.predicate;

                        let object@(Term::NamedNode(_) | Term::Literal(_)) = triple.object else {
                            return Ok(());
                        };

                        let triple = self.compress_rdf_triple(Triple { subject, predicate, object });

                        if !dedup || self.found_new_triple(triple) {
                            tx.send(triple).unwrap();
                        }

                        Ok(())
                    });

                    if let Err(e) = res {
                        eprintln!("{e}")
                    }
                }

                Ok(())
            });

            (writer.join(), reader.join())
        });

        writer_res.unwrap()?;
        reader_res.unwrap()?;

        Ok(())
    }
}
