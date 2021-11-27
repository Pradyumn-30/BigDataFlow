import apache_beam as beam
import re
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class ComputeWordLengthFn(beam.DoFn):
    def process(self, element):
        return re.findall(r'[\w\']+',element, re.UNICODE)

with beam.Pipeline('DirectRunner') as p:
    lines = p | 'Read' >> beam.io.ReadFromText('shakespeare.txt')
    counts = (
        lines
        | 'Split' >> (beam.ParDo(ComputeWordLengthFn()).with_output_types(str))
        | 'PairWIthOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum))

    # Format the counts into a PCollection of strings.
    def format_result(word, count):
      return '%s: %d' % (word, count)

    output = counts | 'Format' >> beam.MapTuple(format_result)
    output | 'Write2' >> beam.io.WriteToText('Word-Count2')
print('File Created')