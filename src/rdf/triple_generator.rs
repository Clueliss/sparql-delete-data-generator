use crate::CompressedRdfTriples;
use chrono::NaiveDateTime;
use rand::Rng;

pub fn random_triple_generator(triples: &[[u64; 3]]) -> impl Iterator<Item = [u64; 3]> + '_ {
    let mut rng = rand::thread_rng();

    std::iter::from_fn(move || {
        let random = rng.gen_range(0..triples.len());
        Some(triples[random])
    })
}

pub fn changeset_triple_generator(
    sorted_changesets: &[(NaiveDateTime, CompressedRdfTriples)],
) -> impl Iterator<Item = [u64; 3]> + '_ {
    let start_off = rand::thread_rng().gen_range(0..sorted_changesets.len());

    sorted_changesets[start_off..]
        .iter()
        .chain(sorted_changesets[..start_off].iter().rev())
        .flat_map(|(_, compressed_triples)| compressed_triples.iter().copied())
}
