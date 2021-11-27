import apache_beam as beam
import re
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class ComputeWordLengthFn(beam.DoFn):
    def process(self, element):
        return re.findall(r'[\w\']+',element, re.UNICODE)

def sortGroupedData(row):
    (keyNumber, sortData) = row
    sortData.sort(key=lambda x: x[1], reverse=True)
    return sortData[0:10]

def addKey(row):
    return (1, row)

with beam.Pipeline('DirectRunner') as p:
    lines = p | 'Read' >> beam.io.ReadFromText('shakespeare.txt')
    counts = (
        lines
        | 'Split' >> (beam.ParDo(ComputeWordLengthFn()).with_output_types(str))
        | 'PairWIthOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum)
        | 'AddKey' >> beam.Map(addKey)
        | 'GroupByKey' >> beam.GroupByKey()
        | 'SortGroupedData' >> beam.Map(sortGroupedData)
        | 'Write' >> WriteToText('./TOP_WORDS.txt')
        )
print('File Created')