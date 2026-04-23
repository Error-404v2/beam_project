"""
Entry point for the profiling pipeline.
Run: python profiling_pipeline.py
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from profiling_logic import build_profiling_pipeline
import os

OUTPUT_FILE = "output/profiling_report.txt"


def run():
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
