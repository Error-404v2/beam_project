"""
Transforms module.

Contains custom DoFn, Map, ParDo, and CombineFn logic.
"""
import apache_beam as beam

class CountFn(beam.CombineFn):
    """Counts elements."""
    def create_accumulator(self):       return 0
    def add_input(self, total, _):      return total + 1
    def merge_accumulators(self, accs): return sum(accs)
    def extract_output(self, total):    return total

class AverageFn(beam.CombineFn):
    """Computes an average."""
    def create_accumulator(self):       return (0, 0)
    def add_input(self, acc, value):    return (acc[0] + value, acc[1] + 1)
    def merge_accumulators(self, accs): return (sum(a[0] for a in accs), sum(a[1] for a in accs))
    def extract_output(self, acc):      return round(acc[0] / acc[1], 1) if acc[1] else 0

class MinMaxFn(beam.CombineFn):
    """Finds min and max."""
    def create_accumulator(self):       return (float("inf"), float("-inf"))
    def add_input(self, acc, value):    return (min(acc[0], value), max(acc[1], value))
    def merge_accumulators(self, accs): return (min(a[0] for a in accs), max(a[1] for a in accs))
    def extract_output(self, acc):      return {"min": acc[0], "max": acc[1]}