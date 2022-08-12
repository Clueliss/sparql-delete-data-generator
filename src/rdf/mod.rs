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

pub fn changeset_file_iter<P: AsRef<Path>>(path: P) -> impl Iterator<Item = walkdir::Result<walkdir::DirEntry>> {
    walkdir::WalkDir::new(path.as_ref())
        .sort_by_file_name()
        .into_iter()
        .filter(move |de| {
            de.as_ref().map(|de| {
                de.file_type().is_file()
                    && matches!(de.path().extension(), Some(ext) if ext == triple_compressor::COMPRESSED_TRIPLE_FILE_EXTENSION)
            }).unwrap_or(true)
        })
}
