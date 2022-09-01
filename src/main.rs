#![feature(hasher_prefixfree_extras, let_else)]

mod rdf;
mod sparql;

use crate::sparql::OutputOrder;
use clap::{ArgEnum, Parser, Subcommand};
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

        /// File to write the query to
        #[clap(short = 'o', long)]
        query_out: PathBuf,

        /// set the order of the generated queries
        #[clap(arg_enum, short = 'r', long, default_value_t = OutputOrder::AsSpecified)]
        output_order: OutputOrder,

        #[clap(subcommand)]
        g_type: GenerateType,

        /// Query specs of the form <N_QUERIES>x<N_TRIPLE_PER_QUERY>
        #[clap(value_parser, global(true))]
        query_specs: Vec<sparql::QuerySpec>,
    },
}

#[derive(Subcommand)]
enum GenerateType {
    /// derives the queries by selecting random triples from the dataset
    Randomized,

    /// derives the queries from a set of changesets
    Changeset {
        /// Path to the directory tree containing the compressed changesets.
        #[clap(short = 'c', long)]
        compressed_changeset_dir: PathBuf,

        /// Query generation type
        #[clap(arg_enum, short = 't', long = "type", default_value_t = GenerateChangesetType::AsIs)]
        generate_type: GenerateChangesetType,
    },
}

#[derive(ArgEnum, Clone)]
enum GenerateChangesetType {
    /// tries to fulfill the requested query sizes as closely as possible
    /// with the existing changesets
    AsIs,

    /// truncates or stitches changesets together to fulfill the requested
    /// sizes exactly
    FixedSize,
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
            query_out,
            query_specs,
            g_type,
            output_order,
        } => {
            println!("loading main dataset...");

            let compressor = unsafe { FrozenRdfTripleCompressor::load_frozen(compressor_state)? };
            let dataset_triples = unsafe { CompressedRdfTriples::load(compressed_dataset)? };

            println!("loaded {} triples from main dataset", dataset_triples.len());

            match g_type {
                GenerateType::Changeset { compressed_changeset_dir, generate_type } => {
                    let changesets: Vec<_> = rdf::changeset_file_iter(compressed_changeset_dir)
                        .map(Result::unwrap)
                        .filter_map(|de| match unsafe { CompressedRdfTriples::load(de.path()) } {
                            Ok(triples) => Some(triples),
                            Err(e) => {
                                eprintln!("Error: unable to open {:?}: {e:?}", de.path());
                                None
                            },
                        })
                        .collect();

                    match generate_type {
                        GenerateChangesetType::AsIs => {
                            println!("generating queries from changesets...");

                            sparql::generate_queries(
                                query_out,
                                query_specs,
                                &compressor,
                                rdf::triple_generator::as_is_changeset_triple_generator(&changesets),
                                output_order,
                            )
                        },
                        GenerateChangesetType::FixedSize => {
                            println!("generating fixed size queries from changesets...");

                            sparql::generate_queries(
                                query_out,
                                query_specs,
                                &compressor,
                                rdf::triple_generator::fixed_size_changeset_triple_generator(
                                    &changesets,
                                    &dataset_triples,
                                ),
                                output_order,
                            )
                        },
                    }?
                },
                GenerateType::Randomized => {
                    println!("generating queries from main dataset...");

                    sparql::generate_queries(
                        query_out,
                        query_specs,
                        &compressor,
                        rdf::triple_generator::random_triple_generator(&dataset_triples),
                        output_order,
                    )?;
                },
            }
        },
    }

    Ok(())
}
