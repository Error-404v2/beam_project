"""
Run the data profiling pipeline.
Usage: python profiling.py
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from profiling_pipeline import build_profiling_pipeline
import os

OUTPUT_FILE = "output/profiling_report.txt"


def run():
    """Create the Beam pipeline, execute profiling, and save the report."""
    try:
        os.remove(OUTPUT_FILE)
    except OSError:
        pass

    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        build_profiling_pipeline(p, OUTPUT_FILE)

    print(f"\nReport saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    run()
