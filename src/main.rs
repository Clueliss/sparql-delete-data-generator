#![feature(iter_advance_by, hasher_prefixfree_extras, let_else, slice_partition_dedup)]

mod rdf;
mod sparql;

use clap::{ArgEnum, Parser, Subcommand};
use memory_mapped::MemoryMapped;
use rdf::triple_compressor::{frozen::FrozenRdfTripleCompressor, CompressedRdfTriples, RdfTripleCompressor};
use sparql::OutputOrder;
use std::{path::PathBuf, str::FromStr};

#[derive(Clone, Copy)]
pub struct QuerySpecOpt {
    n_queries: usize,
    n_triples_per_query: QuerySizeOpt,
}

#[derive(Clone, Copy)]
pub enum QuerySizeOpt {
    Percentage(f64),
    Absolute(usize),
}

impl FromStr for QuerySpecOpt {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (n_queries, n_triples_per_query) = s
            .split_once("x")
            .ok_or_else(|| format!("invalid query spec, expected delimiter"))?;

        let n_queries = n_queries
            .parse()
            .map_err(|e| format!("invalid query spec, first value is not integer: {e:?}"))?;

        let n_triples_per_query = if n_triples_per_query.ends_with('%') {
            QuerySizeOpt::Percentage(
                n_triples_per_query
                    .trim_end_matches('%')
                    .parse::<f64>()
                    .map_err(|e| format!("invalid query spec, triple count specifier is not integer: {e:?}"))?
                    / 100.0,
            )
        } else {
            QuerySizeOpt::Absolute(
                n_triples_per_query
                    .parse()
                    .map_err(|e| format!("invalid query spec, triple count specifier is not integer: {e:?}"))?,
            )
        };

        Ok(QuerySpecOpt { n_queries, n_triples_per_query })
    }
}

impl QuerySizeOpt {
    pub fn get_absolute(self, n_total_triples: usize) -> usize {
        match self {
            QuerySizeOpt::Absolute(n) => n,
            QuerySizeOpt::Percentage(percent) => (n_total_triples as f64 * percent) as usize,
        }
    }
}

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

        /// append to query-out instead of overwriting it
        #[clap(short, long, action)]
        append: bool,

        #[clap(subcommand)]
        g_type: GenerateType,

        /// Query specs of the form <N_QUERIES>x<N_TRIPLE_PER_QUERY>
        #[clap(value_parser, global(true))]
        query_specs: Vec<QuerySpecOpt>,
    },
}

#[derive(Subcommand)]
enum GenerateType {
    /// derives the queries by selecting random triples from the dataset
    Randomized {
        /// allow the generator to generate distinct queries
        /// with common triples
        #[clap(short = 'd', long, action)]
        allow_duplicates: bool,
    },

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

    /// linearly go through all changesets and generate a query for
    /// each changeset
    Linear,
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
            append,
        } => {
            println!("loading main dataset...");

            let compressor = unsafe { FrozenRdfTripleCompressor::load_frozen(compressor_state)? };
            let dataset_triples = unsafe { CompressedRdfTriples::load(compressed_dataset)? };

            println!("loaded {} distinct triples from main dataset", dataset_triples.len());

            let query_specs: Vec<_> = query_specs
                .into_iter()
                .map(|QuerySpecOpt { n_queries, n_triples_per_query }| sparql::QuerySpec {
                    n_queries,
                    n_triples_per_query: n_triples_per_query.get_absolute(dataset_triples.len()),
                })
                .collect();

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
                                append,
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
                                append,
                            )
                        },
                        GenerateChangesetType::Linear => {
                            println!("generating queries by linearly iterating changesets...");

                            if !query_specs.is_empty() {
                                println!("Warning: ignoring query specs in linear generation mode");
                            }

                            sparql::generate_linear_no_size_hint(
                                query_out,
                                &compressor,
                                rdf::triple_generator::linear_changeset_triple_generator(&changesets),
                                append,
                            )
                        },
                    }
                },
                GenerateType::Randomized { allow_duplicates: false } => {
                    println!("generating distinct queries from main dataset...");

                    let total_query_triples: usize = query_specs
                        .iter()
                        .map(|sparql::QuerySpec { n_queries, n_triples_per_query }| n_queries * n_triples_per_query)
                        .sum();

                    sparql::generate_queries(
                        query_out,
                        query_specs,
                        &compressor,
                        rdf::triple_generator::random_distinct_triple_generator(&dataset_triples, total_query_triples),
                        output_order,
                        append,
                    )
                },
                GenerateType::Randomized { allow_duplicates: true } => {
                    println!("generating queries from main dataset...");

                    sparql::generate_queries(
                        query_out,
                        query_specs,
                        &compressor,
                        rdf::triple_generator::random_triple_generator(&dataset_triples),
                        output_order,
                        append,
                    )
                },
            }?
        },
    }

    Ok(())
}
