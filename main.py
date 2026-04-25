"""
Main module.

Entry point: parses args, creates Pipeline, runs it.
"""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from pipeline import build_pipeline
from config import OUTPUT_FILE
import os
import argparse

def run():
    """Create the Beam pipeline, execute profiling, and save the report."""
    
    # Standard argparse setup
    parser = argparse.ArgumentParser()
    # Add any custom pipeline options here in the future
    known_args, pipeline_args = parser.parse_known_args()

    # Clear previous output
    try:
        os.remove(OUTPUT_FILE)
    except OSError:
        pass

    options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options=options) as p:
        build_pipeline(p, OUTPUT_FILE)

    print(f"\nReport saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    run()