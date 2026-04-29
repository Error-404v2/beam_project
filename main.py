import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from pipeline import build_pipeline
from config import DEFAULT_INPUT_SOURCE, INPUT_SOURCES, get_input_paths
from utils import cleanup_temp, normalize_beam_args, setup_logging


def parse_args():
    """Parse app-specific options and leave Beam options untouched."""
    parser = argparse.ArgumentParser(description="Run the hospital dashboard Beam pipeline.")
    parser.add_argument(
        "--input-source",
        choices=sorted(INPUT_SOURCES),
        default=DEFAULT_INPUT_SOURCE,
        help="Read input CSVs from local demo files or the configured GCS bucket.",
    )
    parser.add_argument(
        "--output-mode",
        choices=["bigquery", "local"],
        default="bigquery",
        help="Write to BigQuery or local JSONL files for DirectRunner validation.",
    )
    return parser.parse_known_args()


def run():
    """Create the Beam pipeline and write dashboard-ready outputs."""
    setup_logging()
    
    # 1. Get CLI arguments and input paths.
    args, beam_args = parse_args()
    beam_args = normalize_beam_args(beam_args)
    input_paths = get_input_paths(args.input_source)

    # 2. Construct and execute the Beam pipeline.
    logging.info("Building and starting the Beam pipeline for dashboard ingestion...")
    options = PipelineOptions(beam_args)
    with beam.Pipeline(options=options) as p:
        build_pipeline(p, input_paths, output_mode=args.output_mode)

    # 3. Clean up .egg-info metadata at the end
    cleanup_temp()

    logging.info("Execution complete.")
    logging.info("Input source: %s", args.input_source)
    logging.info("Output mode: %s", args.output_mode)

if __name__ == "__main__":
    run()
