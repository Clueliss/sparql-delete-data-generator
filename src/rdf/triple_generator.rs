use crate::rdf::triple_compressor::{CompressedRdfTriples, TripleElementId};
use rand::{Rng, SeedableRng};
use std::collections::HashSet;

pub fn random_distinct_triple_generator<'a>(
    triples: &'a CompressedRdfTriples,
    n_total_query_triples: usize,
) -> impl FnMut(usize) -> Box<dyn Iterator<Item = &'a [TripleElementId; 3]> + Send + 'a> {
    let mut rng = rand::rngs::SmallRng::from_entropy();
    let mut itr = rand::seq::index::sample(&mut rng, triples.len(), n_total_query_triples).into_iter();

    move |size_hint: usize| {
        let ret = itr.clone().take(size_hint).map(|ix| &triples[ix]);

        let _ = itr.advance_by(size_hint);

        Box::new(ret)
    }
}

pub fn random_triple_generator<'a>(
    triples: &'a CompressedRdfTriples,
) -> impl FnMut(usize) -> Box<dyn Iterator<Item = &'a [TripleElementId; 3]> + Send + 'a> {
    |size_hint: usize| {
        let mut rng = rand::rngs::SmallRng::from_entropy();

        let itr = rand::seq::index::sample(&mut rng, triples.len(), size_hint)
            .into_iter()
            .map(|ix| &triples[ix]);

        Box::new(itr)
    }
}

pub fn fixed_size_changeset_triple_generator<'a, 'c, 'd>(
    changesets: &'c [CompressedRdfTriples],
    dataset: &'d CompressedRdfTriples,
) -> impl FnMut(usize) -> Box<dyn Iterator<Item = &'c [TripleElementId; 3]> + Send + 'a>
where
    'c: 'a,
    'd: 'a,
{
    let start_off = rand::thread_rng().gen_range(0..changesets.len());

    move |size_hint: usize| {
        let itr = changesets[start_off..]
            .iter()
            .chain(changesets[..start_off].iter().rev())
            .flat_map(|compressed_triples| compressed_triples.iter())
            .filter(|triple| dataset.contains(triple))
            .take(size_hint);

        Box::new(itr)
    }
}

pub fn as_is_changeset_triple_generator<'c>(
    changesets: &'c [CompressedRdfTriples],
) -> impl FnMut(usize) -> Box<dyn Iterator<Item = &'c [TripleElementId; 3]> + Send + 'c> {
    let mut used = HashSet::new();

    move |size_hint: usize| {
        let (used_ix, changeset) = changesets
            .iter()
            .enumerate()
            .filter(|(ix, _)| !used.contains(ix))
            .min_by_key(|(_, triples)| triples.len().abs_diff(size_hint))
            .expect("more than 0 changesets");

        println!("using changeset: {used_ix}");

        used.insert(used_ix);

        Box::new(changeset.iter())
    }
}

pub fn linear_changeset_triple_generator<'c>(
    changesets: &'c [CompressedRdfTriples],
) -> impl Iterator<Item = Box<dyn Iterator<Item = &'c [TripleElementId; 3]> + Send + 'c>> {
    let mut cur = 0;

    std::iter::from_fn(move || {
        let ret = changesets.get(cur).map(|chs| Box::new(chs.iter()) as _);
        cur += 1;

        ret
    })
}
