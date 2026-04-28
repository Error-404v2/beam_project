import os
import logging
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from pipeline import build_pipeline
from config import DEFAULT_INPUT_SOURCE, INPUT_SOURCES, get_input_paths
from utils import normalize_beam_args, cleanup_temp, setup_logging

def parse_args():
    """Parse app-specific options and leave Beam options untouched."""
    parser = argparse.ArgumentParser(description="Run the hospital profiling Beam pipeline.")
    parser.add_argument(
        "--input-source",
        choices=sorted(INPUT_SOURCES),
        default=DEFAULT_INPUT_SOURCE,
        help="Read input CSVs from local files or the configured GCS bucket.",
    )
    return parser.parse_known_args()

def run():
    """Create the Beam pipeline, execute profiling, and write exclusively to BigQuery."""
    setup_logging()
    
    # 1. Get CLI arguments and ensure Windows/Cloud compatibility
    args, beam_args = parse_args()
    beam_args = normalize_beam_args(beam_args, args.input_source)
    input_paths = get_input_paths(args.input_source)

    # 2. Construct and execute the Beam pipeline
    logging.info("Building and starting the Beam pipeline for BigQuery ingestion...")
    options = PipelineOptions(beam_args)
    with beam.Pipeline(options=options) as p:
        build_pipeline(p, input_paths)

    # 3. Clean up .egg-info metadata at the end
    cleanup_temp()

    logging.info("Execution complete.")
    logging.info(f"Input source: {args.input_source}")
    logging.info("Output successfully written to BigQuery tables: summary_stats and top_diagnoses.")

if __name__ == "__main__":
    run()
