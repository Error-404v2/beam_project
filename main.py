import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from pipeline import build_pipeline
import os

def run():
    # Force overwrite by removing ONLY the exact file we are about to create
    try:
        os.remove("output/top_diagnoses.txt")
    except OSError:
        pass

    options = PipelineOptions()

    with beam.Pipeline(options=options) as p:
        build_pipeline(p)

if __name__ == "__main__":
    run()