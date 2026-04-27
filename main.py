"""
Main module.

Entry point: creates Pipeline, runs it.
"""
import os
import shutil

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from pipeline import build_pipeline
from config import GENERATED_OUTPUTS, OUTPUT_FILE

def clear_beam_temp_dirs():
    """Remove Beam temporary directories left from local runs."""
    output_dir = os.path.dirname(OUTPUT_FILE)

    def make_writable_and_retry(func, path, _):
        os.chmod(path, 0o700)
        func(path)

    for name in os.listdir(output_dir):
        path = os.path.join(output_dir, name)
        if name.startswith("beam-temp-") and os.path.isdir(path):
            shutil.rmtree(path, onerror=make_writable_and_retry)


def clear_generated_outputs():
    """Remove generated files so Beam reruns do not emit overwrite warnings."""
    for path in GENERATED_OUTPUTS:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    clear_beam_temp_dirs()

def run():
    """Create the Beam pipeline, execute profiling, and save the report."""
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    clear_generated_outputs()

    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        build_pipeline(p, OUTPUT_FILE)

    clear_beam_temp_dirs()

    print(f"\nReport saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    run()