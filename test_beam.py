import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions([
    "--runner=SparkRunner",
    "--streaming"
])

class ComputeWordLengthFn(beam.DoFn):
  def process(self, element):
    print (element)
    return [element]

with beam.Pipeline(options=options) as pipeline:
    lines = (
      pipeline
      | beam.Create([
          'To be, or not to be: that is the question: ',
          "Whether 'tis nobler in the mind to suffer ",
          'The slings and arrows of outrageous fortune, ',
          'Or to take arms against a sea of troubles, ',
      ]*1000))
    word_lengths = lines | beam.ParDo(ComputeWordLengthFn())
    
