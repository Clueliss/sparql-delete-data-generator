use crate::rdf::triple_compressor::decompressor::RdfTripleDecompressor;
use clap::ArgEnum;
use rand::seq::SliceRandom;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{
    borrow::Borrow,
    fs::File,
    hash::Hash,
    io::{BufWriter, Write},
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Copy, Clone, ArgEnum)]
pub enum OutputOrder {
    AsSpecified,
    Randomized,
    SortedSizeAsc,
    SortedSizeDesc,
}

#[derive(Clone, Copy)]
pub struct QuerySpec {
    pub n_queries: usize,
    pub n_triples_per_query: usize,
}

pub fn generate_queries<P, Q, F, I, T>(
    out_file: P,
    query_specs: Q,
    decompressor: &RdfTripleDecompressor,
    mut triple_generator_factory: F,
    order: OutputOrder,
    append: bool,
) -> std::io::Result<()>
where
    P: AsRef<Path>,
    Q: IntoIterator<Item = QuerySpec>,
    F: FnMut(usize) -> I,
    I: Iterator<Item = T> + Send,
    T: Borrow<[u64; 3]> + Eq + Hash + Send,
{
    let generators: Vec<_> = {
        let mut tmp: Vec<_> = query_specs
            .into_iter()
            .flat_map(|QuerySpec { n_queries, n_triples_per_query }| {
                std::iter::repeat(n_triples_per_query).take(n_queries)
            })
            .map(|n_triples| (n_triples, triple_generator_factory(n_triples)))
            .collect();

        match order {
            OutputOrder::AsSpecified => (),
            OutputOrder::Randomized => tmp.shuffle(&mut rand::thread_rng()),
            OutputOrder::SortedSizeAsc => tmp.sort_unstable_by_key(|(n_triples, _)| *n_triples),
            OutputOrder::SortedSizeDesc => tmp.sort_unstable_by_key(|(n_triples, _)| std::cmp::Reverse(*n_triples)),
        }

        tmp
    };

    let queries: Vec<_> = generators
        .into_par_iter()
        .map(|(n_triples, triple_generator)| {
            let remove_set: Vec<_> = triple_generator
                .map(|triple| {
                    decompressor
                        .decompress_rdf_triple(triple.borrow())
                        .expect("to use same compressor as used for compression")
                })
                .collect();

            if remove_set.len() != n_triples {
                println!(
                    "Warning: requested query size {n_triples} cannot be fulfilled closest available size is {}",
                    remove_set.len()
                );
            }

            remove_set
        })
        .collect();

    write_delete_data_queries(out_file, append, queries)
}

pub fn generate_linear_no_size_hint<P, F, I, T>(
    out_file: P,
    decompressor: &RdfTripleDecompressor,
    triple_generator_factory: F,
    append: bool,
    //dataset_triples: &CompressedRdfTriples,
) -> std::io::Result<()>
where
    P: AsRef<Path>,
    F: IntoIterator<Item = I>,
    I: Iterator<Item = T> + Send,
    T: Borrow<[u64; 3]> + Eq + Hash + Send,
{
    let generators: Vec<_> = triple_generator_factory.into_iter().collect();

    let n = AtomicUsize::new(0);

    let queries: Vec<Vec<_>> = generators
        .into_par_iter()
        .map(|triple_generator| {
            let triples: Vec<_> = triple_generator
                /*.inspect(|t| {
                    if dataset_triples.contains(t.borrow()) {
                        n.fetch_add(1, Ordering::SeqCst);
                    }
                })*/
                .map(|triple| {
                    decompressor
                        .decompress_rdf_triple(triple.borrow())
                        .expect("to use same compressor as used for compression")
                })
                .collect();

            triples
        })
        .collect();

    println!("{}", n.load(Ordering::SeqCst));

    write_delete_data_queries(out_file, append, queries)
}

fn write_delete_data_queries<P>(out_file: P, append: bool, queries: Vec<Vec<[&str; 3]>>) -> std::io::Result<()>
where
    P: AsRef<Path>,
{
    let f = File::options()
        .append(append)
        .truncate(!append)
        .create(true)
        .write(true)
        .open(out_file)?;

    let mut writer = BufWriter::new(f);

    for query in queries {
        write!(writer, "DELETE DATA {{ ")?;

        for [s, p, o] in query {
            write!(writer, "{s} {p} {o} . ")?;
        }

        writeln!(writer, "}}")?;
    }

    Ok(())
}
