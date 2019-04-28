from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='gs://us-central1-maxious-airflow-64b78389-bucket/data/benchmark_69211100_20190425.csv',
    #'../../data/benchmark_69211100_20190425.csv',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        default='gs://us-central1-maxious-airflow-64b78389-bucket/data/tally_69211100_20190425.csv',
    #'../../data/tally_69211100_20190425.csv',
                        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    lines = p | 'read' >> ReadFromText(known_args.input, skip_header_lines=1)

    # Count the occurrences of each path.
    def count_ones(path_ones):
        (path, ones) = path_ones
        return (path, sum(ones))

    counts = (lines
              | 'split' >> beam.Map(lambda x: x.split(',')[-1])
              | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
              | 'group' >> beam.GroupByKey()
              | 'count' >> beam.Map(count_ones))

    # Format the counts into a PCollection of strings.
    def format_result(path_count):
        (path, count) = path_count
        return '%s,%d' % (path, count)

    output = counts | 'format' >> beam.Map(format_result)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | 'write' >> WriteToText(known_args.output, header='path,hits')

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()