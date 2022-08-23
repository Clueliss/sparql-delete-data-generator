#![feature(hasher_prefixfree_extras, let_else)]

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
        #[clap(short = 'i', long)]
        previous_compressor_state: Option<PathBuf>,

        /// Path to file in which the resulting compressor state should be written.
        /// Defaults to same path as previous-compressor-state if provided
        #[clap(short = 'o', long, required_unless_present("previous-compressor-state"))]
        compressor_state_out: Option<PathBuf>,

        /// Operate recursively on directories
        #[clap(short = 'r', long)]
        recursive: bool,

        /// Datasets to compress
        datasets: Vec<PathBuf>,
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
        #[clap(short = 'c', long)]
        compressed_changeset_dir: Option<PathBuf>,

        /// File to write the query to
        #[clap(short = 'o', long)]
        query_out: PathBuf,

        /// Query specs of the form <N_QUERIES>x<N_TRIPLE_PER_QUERY>
        query_specs: Vec<sparql::QuerySpec>,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();

    match opts {
        Opts::Compress { previous_compressor_state, compressor_state_out, recursive, datasets } => {
            let mut compressor = if let Some(pcs) = &previous_compressor_state {
                let frozen = unsafe { FrozenRdfTripleCompressor::load_frozen(pcs)? };
                RdfTripleCompressor::from_frozen(frozen)
            } else {
                RdfTripleCompressor::new()
            };

            for dataset in datasets {
                if recursive && dataset.is_dir() {
                    for file in walkdir::WalkDir::new(dataset) {
                        let file = file?;

                        if file.file_type().is_file() && matches!(file.path().extension(), Some(ext) if ext == "nt") {
                            compressor.compress_rdf_triple_file(file.path())?;
                        }
                    }
                } else {
                    compressor.compress_rdf_triple_file(dataset)?;
                }
            }

            compressor.freeze(compressor_state_out.unwrap_or_else(|| {
                previous_compressor_state.expect("previous compressor state if no compressor out specified")
            }))?;
        },
        Opts::Generate {
            compressor_state,
            compressed_dataset,
            compressed_changeset_dir,
            query_out,
            query_specs,
        } => {
            println!("loading main dataset...");

            let compressor = unsafe { FrozenRdfTripleCompressor::load_frozen(compressor_state)? };
            let dataset_triples = unsafe { CompressedRdfTriples::load(compressed_dataset)? };

            println!("loaded {} triples from main dataset", dataset_triples.len());

            if let Some(changeset_dir) = compressed_changeset_dir {
                println!("generating queries from changesets...");

                let changesets: Vec<_> = rdf::changeset_file_iter(changeset_dir)
                    .map(Result::unwrap)
                    .filter_map(|de| match unsafe { CompressedRdfTriples::load(de.path()) } {
                        Ok(triples) => Some(triples),
                        Err(e) => {
                            eprintln!("Error: unable to open {:?}: {e:?}", de.path());
                            None
                        }
                    })
                    .collect();

                sparql::generate_queries(query_out, &compressor, query_specs, || {
                    rdf::triple_generator::changeset_triple_generator(&changesets)
                        .filter(|triple| dataset_triples.contains(triple))
                })?;
            } else {
                println!("generating queries from main dataset...");

                sparql::generate_queries(query_out, &compressor, query_specs, || {
                    rdf::triple_generator::random_triple_generator(&dataset_triples)
                })?;
            }
        },
    }

    Ok(())
}
