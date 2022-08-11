use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use std::path::Path;

pub mod triple_compressor;
pub mod triple_generator;

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
        dir_entry.file_type().is_file()
            && matches!(dir_entry.path().extension(), Some(ext) if ext == triple_compressor::COMPRESSED_TRIPLE_FILE_EXTENSION)
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
