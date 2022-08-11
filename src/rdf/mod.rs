use crate::MemoryMapped;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use std::{ops::Deref, path::Path};

pub mod triple_compressor;
pub mod triple_generator;

pub struct CompressedRdfTriples(MemoryMapped<[[u64; 3]]>);

impl CompressedRdfTriples {
    pub fn load<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        MemoryMapped::open(path).map(CompressedRdfTriples)
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

pub fn split_rdf_triple(triple: &str) -> Option<[&str; 3]> {
    let mut split = triple.splitn(3, ' ');

    let subject = split.next()?;
    let predicate = split.next()?;
    let object = split.next()?.trim_end_matches(" .");

    Some([subject, predicate, object])
}

pub fn changeset_file_iter<P: AsRef<Path>>(
    path: P,
) -> impl Iterator<Item = walkdir::Result<(NaiveDateTime, walkdir::DirEntry)>> {
    let is_nt_file = |dir_entry: &walkdir::DirEntry| {
        dir_entry.file_type().is_file() && matches!(dir_entry.path().extension(), Some(ext) if ext == "compressed")
    };

    walkdir::WalkDir::new(path)
        .min_depth(5)
        .into_iter()
        .filter_entry(is_nt_file)
        .map(|changeset| {
            changeset.map(|changeset| {
                let mut date = changeset
                    .path()
                    .components()
                    .rev()
                    .skip(1)
                    .take(4)
                    .map(|c| c.as_os_str().to_str().unwrap());

                let hour = date.next().unwrap().parse().unwrap();
                let day = date.next().unwrap().parse().unwrap();
                let month = date.next().unwrap().parse().unwrap();
                let year = date.next().unwrap().parse().unwrap();

                let datetime =
                    NaiveDateTime::new(NaiveDate::from_ymd(year, month, day), NaiveTime::from_hms(hour, 0, 0));

                (datetime, changeset)
            })
        })
}
