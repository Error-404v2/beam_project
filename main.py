import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from pipeline import build_pipeline

def run():
    options = PipelineOptions()

    with beam.Pipeline(options=options) as p:
        build_pipeline(p)

if __name__ == "__main__":
    run()