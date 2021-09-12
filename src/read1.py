"""A example to read records on mysql.

usage:
python -m read1

links:
    https://github.com/esakik/beam-mysql-connector/blob/master/examples/read_records_pipeline.py

"""
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_mysql.connector import splitters
from beam_mysql.connector.io import ReadFromMySQL


class ReadRecordsOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        query    = 'select * from {}.stg_mty_metro_area;'.format( os.environ[ 'MYSQL_NAME' ] )

        host     = os.environ[ 'MYSQL_HOST'     ]
        database = os.environ[ 'MYSQL_NAME'     ]
        user     = os.environ[ 'MYSQL_USER'     ]
        password = os.environ[ 'MYSQL_PASSWORD' ]
        output   = '~/data_lake/out/stg_mty_metro_area'


        parser.add_value_provider_argument("--host"     , dest="host"    , default = host       )
        parser.add_value_provider_argument("--port"     , dest="port"    , default = 3307       )
        parser.add_value_provider_argument("--database" , dest="database", default = database   )
        parser.add_value_provider_argument("--query"    , dest="query"   , default = query      )
        parser.add_value_provider_argument("--user"     , dest="user"    , default = user       )
        parser.add_value_provider_argument("--password" , dest="password", default = password   )
        parser.add_value_provider_argument("--output"   , dest="output"  , default = output     )


def run():
    options = ReadRecordsOptions()

    p = beam.Pipeline(options=options)

    read_from_mysql = ReadFromMySQL(
        query    = options.query,
        host     = options.host,
        database = options.database,
        user     = options.user,
        password = options.password,
        port     = options.port,
        splitter = splitters.NoSplitter(),  # you can select how to split query from splitters
    )

    (
        p
        | "ReadFromMySQL" >> read_from_mysql
        | "NoTransform" >> beam.Map(lambda e: e)
        | "WriteToText" >> beam.io.WriteToText(options.output, file_name_suffix=".txt", shard_name_template="")
    )

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()