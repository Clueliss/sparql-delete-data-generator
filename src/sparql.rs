use crate::FrozenRdfTripleCompressor;
use clap::ArgEnum;
use rand::seq::SliceRandom;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{
    borrow::Borrow,
    collections::HashSet,
    fs::File,
    hash::{BuildHasherDefault, Hash},
    io::{BufWriter, Write},
    path::Path,
    str::FromStr,
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

impl FromStr for QuerySpec {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (n_queries, n_triples_per_query) = s
            .split_once("x")
            .ok_or_else(|| format!("invalid query spec, expected delimiter"))?;

        let n_queries = n_queries
            .parse()
            .map_err(|e| format!("invalid query spec, first value is not integer: {e:?}"))?;

        let n_triples_per_query = n_triples_per_query
            .parse()
            .map_err(|e| format!("invalid query spec, triple count specifier is not integer: {e:?}"))?;

        Ok(QuerySpec { n_queries, n_triples_per_query })
    }
}

pub fn generate_queries<P, Q, F, I, T>(
    out_file: P,
    query_specs: Q,
    compressor: &FrozenRdfTripleCompressor,
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
        .map(|(n_triples, mut triple_generator)| {
            let mut remove_set: HashSet<_, BuildHasherDefault<ahash::AHasher>> = HashSet::default();

            while let Some(triple) = triple_generator.next() {
                remove_set.insert(triple);
            }

            if remove_set.len() != n_triples {
                println!(
                    "Warning: requested query size {n_triples} cannot be fulfilled closest available size is {}",
                    remove_set.len()
                );
            }

            remove_set
        })
        .collect();

    let f = File::options()
        .append(append)
        .create(true)
        .write(true)
        .open(out_file)?;

    let mut bw = BufWriter::new(f);

    for query in queries {
        write_delete_data_query(&mut bw, query, compressor)?;
    }

    Ok(())
}

fn write_delete_data_query<W, T>(
    writer: &mut W,
    triples: impl IntoIterator<Item = T>,
    compressor: &FrozenRdfTripleCompressor,
) -> std::io::Result<()>
where
    W: Write,
    T: Borrow<[u64; 3]>,
{
    writeln!(writer, "DELETE DATA {{")?;

    for triple in triples {
        let [s, p, o] = compressor
            .decompress_rdf_triple(triple.borrow())
            .expect("to use same compressor as used for compression");

        writeln!(writer, "  {s} {p} {o} .")?;
    }

    writeln!(writer, "}}\n")
}
