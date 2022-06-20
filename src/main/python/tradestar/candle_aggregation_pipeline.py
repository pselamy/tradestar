import apache_beam as beam
from apache_beam.options import pipeline_options


class CandleAggregationPipelineOptions(pipeline_options.PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--abc', default='start')
        parser.add_argument('--xyz', default='end')


def run(save_main_session=True):
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    options = CandleAggregationPipelineOptions(save_main_session=save_main_session)

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=options) as p:
        pass


if __name__ == '__main__':
    run()
