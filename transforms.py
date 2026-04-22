import apache_beam as beam

class ToUpperCase(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.Map(lambda x: x.upper())