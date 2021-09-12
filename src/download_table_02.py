'''
description: This pipeline moves data from mysql to txt file.
              Where each line represent a dictionary.


usage
python -m download_table_02 --input stg_mty_metro_area  --output ~/data_lake/out/stg_mty_metro_area 

links:

https://pypi.org/project/PyMySQL/
https://github.com/esakik/beam-mysql-connector

'''




"""A mysql table download workflow."""

# pytype: skip-file

import argparse
import logging
import re
import os
import json

import apache_beam as beam
from apache_beam.io                         import ReadFromText
from apache_beam.io                         import WriteToText
from apache_beam.options.pipeline_options   import PipelineOptions
from apache_beam.options.pipeline_options   import SetupOptions

from beam_mysql.connector                   import splitters
from beam_mysql.connector.io                import ReadFromMySQL





class BuildRowDoFn(beam.DoFn):
  """Parse each line of dictionary and make a string with the value of the fields"""
  def process(self, element):
    """Returns an iterator over the words of this element.
    The element is a line of text.  If the line is blank, note that, too.
    Args:
      element: the element being processed
    Returns:
      The processed element.
    """
    #return re.findall(r'[\w\']+', element, re.UNICODE)
    #words = re.findall(r'[\w\']+', element, re.UNICODE)
    print( '\n PrintRowsDoFn.process()... \n' )
    s = '{min_cp}|{max_cp}|{D_mnpio}'.format( **element )    

    return [s]


def get_query( table ):
    database = os.environ[ 'MYSQL_NAME'     ]
    query    = "SELECT * FROM {}.{};".format( database, table )
    return query


def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  query = get_query( known_args.input )
  print( '\n query: {} \n'.format( query ) )


  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:


    read_from_mysql = ReadFromMySQL(
        query    = query,
        host     = os.environ[ 'MYSQL_HOST'     ],
        database = os.environ[ 'MYSQL_NAME'     ],
        user     = os.environ[ 'MYSQL_USER'     ],
        password = os.environ[ 'MYSQL_PASSWORD' ],
        port     = 3306,
        splitter = splitters.NoSplitter()  # you can select how to split query for performance
    )

    print( '\n\n printing rows ... begin \n\n' )
    print( 'type( read_from_mysql ): {}'.format( read_from_mysql ) )

    (
        p
        | "ReadFromMySQL" >> read_from_mysql
        | "build string row" >> beam.ParDo( BuildRowDoFn() )
        | "WriteToText" >> beam.io.WriteToText(known_args.output, file_name_suffix=".csv", shard_name_template="")
    )

    #p.run().wait_until_finish()
    
    print( '\n\n printing rows ... end \n\n' )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()