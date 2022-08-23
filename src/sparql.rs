use crate::FrozenRdfTripleCompressor;
use std::{
    collections::HashSet,
    fs::File,
    hash::BuildHasherDefault,
    io::{BufWriter, Write},
    path::Path,
    str::FromStr,
};
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;

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

pub fn generate_queries<F, I, Q, P>(
    out_file: P,
    compressor: &FrozenRdfTripleCompressor,
    query_specs: Q,
    triple_generator_factory: F,
) -> std::io::Result<()>
where
    F: Fn() -> I + Sync,
    I: Iterator<Item = [u64; 3]>,
    Q: IntoParallelIterator<Item = QuerySpec>,
    P: AsRef<Path>,
{
    let queries: Vec<_> = query_specs
        .into_par_iter()
        .flat_map(|QuerySpec { n_queries, n_triples_per_query }| {
            println!("now generating {n_queries}x{n_triples_per_query} query set...");

            let ref_tgf = &triple_generator_factory;
            (0..n_queries)
                .into_par_iter()
                .map(move |_| {
                    let mut triple_generator = ref_tgf();

                    let mut remove_set: HashSet<_, BuildHasherDefault<ahash::AHasher>> = HashSet::default();

                    while remove_set.len() < n_triples_per_query && let Some(triple) = triple_generator.next() {
                        remove_set.insert(triple);
                    }

                    if remove_set.len() < n_triples_per_query {
                        println!("Warning: not enough triples available to generate query of size {n_triples_per_query} available count is {}", remove_set.len());
                    }

                    remove_set
                })
        })
        .collect();

    let mut bw = BufWriter::new(File::create(out_file)?);

    for query in queries {
        write_delete_data_query(&mut bw, query, compressor)?;
    }

    Ok(())
}

fn write_delete_data_query<W: Write>(
    writer: &mut W,
    triples: impl IntoIterator<Item = [u64; 3]>,
    compressor: &FrozenRdfTripleCompressor,
) -> std::io::Result<()> {
    writeln!(writer, "DELETE DATA {{")?;

    for triple in triples {
        let [s, p, o] = compressor
            .decompress_rdf_triple(&triple)
            .expect("to use same compressor as used for compression");

        writeln!(writer, "  {s} {p} {o} .")?;
    }

    writeln!(writer, "}}\n")
}
