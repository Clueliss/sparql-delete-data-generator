#![feature(hasher_prefixfree_extras)]

mod rdf;

use clap::{Parser, Subcommand};
use memory_mapped::MemoryMapped;
use rdf::triple_compressor::{frozen::FrozenRdfTripleCompressor, RdfTripleCompressor};
use std::{
    collections::HashSet,
    fs::File,
    hash::BuildHasherDefault,
    io::{BufWriter, Write},
    path::PathBuf,
};

#[derive(Parser)]
struct Opts {
    #[clap(short, long)]
    dataset: PathBuf,

    #[clap(subcommand)]
    action: Action,

    #[clap(long)]
    changeset_dir: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Action {
    Compress {
        #[clap(long)]
        previous_compressor_state: Option<PathBuf>,
    },
    Select {
        #[clap(short = 'n', long)]
        count: usize,

        #[clap(short = 'o', long)]
        out_file: PathBuf,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();

    match opts.action {
        Action::Compress { previous_compressor_state } => {
            let mut compressor = if let Some(pcs) = previous_compressor_state {
                let frozen = FrozenRdfTripleCompressor::load_frozen(pcs)?;
                RdfTripleCompressor::from_frozen(frozen)
            } else {
                RdfTripleCompressor::new()
            };

            compressor.compress_rdf_triple_file(&opts.dataset)?;

            if let Some(changeset_dir) = &opts.changeset_dir {
                for changeset in rdf::changeset_file_iter(changeset_dir) {
                    let (_, changeset) = changeset.unwrap();
                    compressor.compress_rdf_triple_file(changeset.path())?;
                }
            }

            compressor.freeze(opts.dataset.with_extension("frozen_translations"))?;
        },
        Action::Select { count, out_file } => {
            println!("loading main dataset...");

            let compressor =
                FrozenRdfTripleCompressor::load_frozen(opts.dataset.with_extension("frozen_translations"))?;

            let dataset_triples = rdf::CompressedRdfTriples::load(opts.dataset.with_extension("compressed"))?;

            println!("loaded {} triples from main dataset", dataset_triples.len());

            let remove_set: HashSet<_, BuildHasherDefault<ahash::AHasher>> =
                if let Some(changeset_dir) = &opts.changeset_dir {
                    println!("selecting triples from changesets...");

                    let changesets = {
                        let mut tmp: Vec<_> = rdf::changeset_file_iter(changeset_dir).filter_map(Result::ok).collect();

                        tmp.sort_unstable_by_key(|(datetime, _)| *datetime);
                        tmp
                    };

                    let mut remove_set = HashSet::default();
                    let mut generator = rdf::triple_generator::changeset_triple_generator(&changesets)
                        .filter(|triple| dataset_triples.contains(triple));

                    while remove_set.len() < count && let Some(triple) = generator.next() {
                    remove_set.insert(triple);
                }

                    remove_set
                } else {
                    println!("selecting random triples...");

                    rdf::triple_generator::random_triple_generator(&dataset_triples)
                        .take(count)
                        .collect()
                };

            println!("selected {} triples", remove_set.len());
            println!("generating query...");

            let mut bw = BufWriter::new(File::create(out_file)?);
            write_delete_data_query(&mut bw, remove_set, &compressor)?;
        },
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

    writeln!(writer, "}}")
}
