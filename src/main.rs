#![feature(hasher_prefixfree_extras)]

mod rdf;
mod sparql;

use clap::Parser;
use memory_mapped::MemoryMapped;
use rdf::triple_compressor::{frozen::FrozenRdfTripleCompressor, CompressedRdfTriples, RdfTripleCompressor};
use std::path::PathBuf;

#[derive(Parser)]
#[clap(author, version, about)]
enum Opts {
    /// Compress an n-triples dataset
    Compress {
        /// Path to an existing compressor state to be used to compress more data
        #[clap(short = 's', long)]
        previous_compressor_state: Option<PathBuf>,

        /// Dataset to compress
        #[clap(short = 'i', long)]
        dataset: PathBuf,

        /// Path to directory tree containing all changesets.
        /// Expected tree structure: year/month/day/hour/changeset
        #[clap(short = 'c', long)]
        changeset_dir: Option<PathBuf>,
    },
    /// Generate SPARQL DELETE DATA queries from a compressed dataset
    Generate {
        /// Path to the associated compressor state
        #[clap(short = 's', long)]
        compressor_state: PathBuf,

        /// Path to the compressed dataset
        #[clap(short = 'i', long)]
        compressed_dataset: PathBuf,

        /// Path to the directory tree containing the compressed changesets.
        /// Expected tree structure: year/month/day/hour/changeset
        #[clap(short = 'c', long)]
        compressed_changeset_dir: Option<PathBuf>,

        /// File to write the query to
        #[clap(short = 'o', long)]
        out_file: PathBuf,

        /// Query specs of the form <N_QUERIES>x<N_TRIPLE_PER_QUERY>
        query_specs: Vec<sparql::QuerySpec>,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();

    match opts {
        Opts::Compress { previous_compressor_state, dataset, changeset_dir } => {
            let mut compressor = if let Some(pcs) = previous_compressor_state {
                let frozen = FrozenRdfTripleCompressor::load_frozen(pcs)?;
                RdfTripleCompressor::from_frozen(frozen)
            } else {
                RdfTripleCompressor::new()
            };

            compressor.compress_rdf_triple_file(&dataset)?;

            if let Some(changeset_dir) = changeset_dir {
                for changeset in rdf::changeset_file_iter(changeset_dir) {
                    let (_, changeset) = changeset.unwrap();
                    compressor.compress_rdf_triple_file(changeset.path())?;
                }
            }

            compressor.freeze(dataset)?;
        },
        Opts::Generate {
            compressor_state,
            compressed_dataset,
            compressed_changeset_dir,
            out_file,
            query_specs,
        } => {
            println!("loading main dataset...");

            let compressor = FrozenRdfTripleCompressor::load_frozen(compressor_state)?;

            let dataset_triples = CompressedRdfTriples::load(compressed_dataset)?;

            println!("loaded {} triples from main dataset", dataset_triples.len());

            if let Some(changeset_dir) = compressed_changeset_dir {
                println!("generating queries from changesets...");

                let changesets = {
                    let mut tmp: Vec<_> = rdf::changeset_file_iter(changeset_dir)
                        .filter_map(Result::ok)
                        .filter_map(|(datetime, de)| {
                            CompressedRdfTriples::load(de.path())
                                .map(|triples| (datetime, triples))
                                .ok()
                        })
                        .collect();

                    tmp.sort_unstable_by_key(|(datetime, _)| *datetime);
                    tmp
                };

                sparql::generate_queries(out_file, &compressor, query_specs, || {
                    rdf::triple_generator::changeset_triple_generator(&changesets)
                        .filter(|triple| dataset_triples.contains(triple))
                })?;
            } else {
                println!("generating queries from main dataset...");

                sparql::generate_queries(out_file, &compressor, query_specs, || {
                    rdf::triple_generator::random_triple_generator(&dataset_triples)
                })?;
            }
        },
    }

    Ok(())
}
