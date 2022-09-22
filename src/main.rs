#![feature(
    iter_advance_by,
    hasher_prefixfree_extras,
    let_else,
    slice_partition_dedup,
    is_sorted
)]

mod rdf;
mod sparql;

use clap::{ArgEnum, Parser, Subcommand};
use memory_mapped::MemoryMapped;
use rdf::triple_compressor::{
    compressor::RdfTripleCompressor, decompressor::RdfTripleDecompressor, CompressedRdfTriples,
    COMPRESSED_TRIPLE_FILE_EXTENSION, UNCOMPRESSED_TRIPLE_FILE_EXTENSION,
};
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
    /// Compress n-triples datasets
    Compress {
        /// Path to an existing compressor state to be used to compress more data
        #[clap(short = 'i', long)]
        previous_compressor_state: Option<PathBuf>,

        /// Path to file in which the resulting compressor state should be written.
        /// Defaults to same path as previous-compressor-state if provided
        #[clap(short = 'o', long, required_unless_present("previous-compressor-state"))]
        compressor_state_out: Option<PathBuf>,

        /// Operate recursively on directories
        #[clap(short = 'r', long, action)]
        recursive: bool,

        #[clap(short = 'D', long, action)]
        dedup: bool,

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

        /// Set the order of the generated queries
        #[clap(arg_enum, short = 'r', long, default_value_t = OutputOrder::AsSpecified)]
        output_order: OutputOrder,

        /// Append to query-out instead of overwriting it
        #[clap(short, long, action)]
        append: bool,

        #[clap(subcommand)]
        g_type: GenerateType,

        /// Query specs of the form <N_QUERIES>x<N_TRIPLE_PER_QUERY>
        #[clap(value_parser, global(true))]
        query_specs: Vec<QuerySpecOpt>,
    },
    /// Generate SPARQL DELETE DATA queries by replicating the given compressed datasets
    Replicate {
        /// Path to the associated compressor state
        #[clap(short = 's', long)]
        compressor_state: PathBuf,

        /// File to write the query to
        #[clap(short = 'o', long)]
        query_out: PathBuf,

        /// Operate recursively on directories
        #[clap(short = 'r', long, action)]
        recursive: bool,

        /// Append to query-out instead of overwriting it
        #[clap(short, long, action)]
        append: bool,

        /// The datasets to replicate
        compressed_datasets: Vec<PathBuf>,
    },
    /// Decompress compressed datasets back into n-triple files
    Decompress {
        /// Path to the associated compressor state
        #[clap(short = 's', long)]
        compressor_state: PathBuf,

        /// Operate recursively on directories
        #[clap(short = 'r', long, action)]
        recursive: bool,

        /// The datasets to replicate
        compressed_datasets: Vec<PathBuf>,
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
        /// Path to the compressed changeset file or directory tree containing the compressed changesets.
        #[clap(short = 'c', long)]
        compressed_changesets: PathBuf,

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

fn dataset_iter(
    paths: Vec<PathBuf>,
    recursive: bool,
    extension: &str,
) -> impl Iterator<Item = walkdir::Result<PathBuf>> + '_ {
    paths.into_iter().flat_map(move |path| {
        if path.is_dir() {
            if recursive {
                walkdir::WalkDir::new(path)
                    .into_iter()
                    .filter_map(|e| match e {
                        Ok(e)
                            if e.file_type().is_file()
                                && matches!(e.path().extension(), Some(ext) if ext == extension) =>
                        {
                            Some(Ok(e.into_path()))
                        },
                        Ok(_) => None,
                        other => Some(other.map(|e| e.into_path())),
                    })
                    .collect()
            } else {
                vec![]
            }
        } else {
            vec![Ok(path)]
        }
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();

    match opts {
        Opts::Compress {
            previous_compressor_state,
            compressor_state_out,
            recursive,
            dedup,
            datasets,
        } => {
            let compressor_state_out = compressor_state_out.as_ref().unwrap_or_else(|| {
                previous_compressor_state
                    .as_ref()
                    .expect("previous compressor state if no compressor out specified")
            });

            let mut compressor = if let Some(pcs) = &previous_compressor_state {
                println!("loading previous compressor state...");
                let frozen = unsafe { RdfTripleDecompressor::load_state(pcs)? };
                RdfTripleCompressor::from_decompressor(frozen)
            } else {
                RdfTripleCompressor::new()
            };

            for dataset in dataset_iter(datasets, recursive, UNCOMPRESSED_TRIPLE_FILE_EXTENSION) {
                let dataset = dataset?;

                println!("compressing {:?}...", dataset);
                compressor.compress_rdf_triple_file(dataset, dedup)?;
            }

            println!("saving compressor state...");
            compressor.save_state(compressor_state_out)?;
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
            println!("loading compressor state...");
            let decompressor = unsafe { RdfTripleDecompressor::load_state(compressor_state)? };

            println!("loading main dataset...");
            let dataset_triples = unsafe { CompressedRdfTriples::load(compressed_dataset)? };
            assert!(
                dataset_triples.is_sorted(),
                "dataset triples must be compressed with -D to ensure correct query generation"
            );

            println!("loaded {} distinct triples from main dataset", dataset_triples.len());

            let query_specs: Vec<_> = query_specs
                .into_iter()
                .map(|QuerySpecOpt { n_queries, n_triples_per_query }| sparql::QuerySpec {
                    n_queries,
                    n_triples_per_query: n_triples_per_query.get_absolute(dataset_triples.len()),
                })
                .collect();

            match g_type {
                GenerateType::Changeset { compressed_changesets: compressed_changeset_dir, generate_type } => {
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
                                &decompressor,
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
                                &decompressor,
                                rdf::triple_generator::fixed_size_changeset_triple_generator(
                                    &changesets,
                                    &dataset_triples,
                                ),
                                output_order,
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
                        &decompressor,
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
                        &decompressor,
                        rdf::triple_generator::random_triple_generator(&dataset_triples),
                        output_order,
                        append,
                    )
                },
            }?
        },
        Opts::Replicate { compressor_state, query_out, recursive, append, compressed_datasets } => {
            println!("loading compressor state...");
            let decompressor = unsafe { RdfTripleDecompressor::load_state(compressor_state)? };

            println!("loading datasets to replicate...");
            let datasets: Vec<_> = dataset_iter(compressed_datasets, recursive, COMPRESSED_TRIPLE_FILE_EXTENSION)
                .map(Result::unwrap)
                .filter_map(|p| match unsafe { CompressedRdfTriples::load(&p) } {
                    Ok(triples) => Some(triples),
                    Err(e) => {
                        eprintln!("Error: unable to open {p:?}: {e:?}");
                        None
                    },
                })
                .collect();

            println!("generating queries by linearly replicating datasets...");
            sparql::generate_linear_no_size_hint(
                query_out,
                &decompressor,
                rdf::triple_generator::linear_changeset_triple_generator(&datasets),
                append,
                //&dataset_triples
            )?;
        },
        Opts::Decompress { compressor_state, recursive, compressed_datasets } => {
            println!("loading compressor state...");
            let decompressor = unsafe { RdfTripleDecompressor::load_state(compressor_state)? };

            for dataset in dataset_iter(compressed_datasets, recursive, COMPRESSED_TRIPLE_FILE_EXTENSION) {
                let dataset = dataset?;

                println!("decompressing {dataset:?}...");
                decompressor.decompress_rdf_triple_file(dataset)?;
            }
        },
    }

    Ok(())
}
