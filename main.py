import sys
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from pipeline import build_pipeline
from config import get_input_paths, GCS_TEMP_LOCATION
from utils import cleanup_temp, normalize_beam_args, setup_logging


def run():
    """Create the Beam pipeline and write dashboard-ready outputs."""
    setup_logging()
    
    # 1. Get arguments and input paths.
    beam_args = normalize_beam_args(sys.argv[1:])
    # Add temp_location for BigQuery FILE_LOADS writes
    beam_args.extend(["--temp_location", GCS_TEMP_LOCATION])
    
    input_paths = get_input_paths()

    # 2. Construct and execute the Beam pipeline.
    logging.info("Building and starting the Beam pipeline for dashboard ingestion...")
    options = PipelineOptions(beam_args)
    with beam.Pipeline(options=options) as p:
        build_pipeline(p, input_paths)

    # 3. Clean up .egg-info metadata at the end
    cleanup_temp()

    logging.info("Execution complete.")

if __name__ == "__main__":
    run()
