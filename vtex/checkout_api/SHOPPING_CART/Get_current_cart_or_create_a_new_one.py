import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict
import argparse
import time
import logging
import json
import typing
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.trigger import AfterWatermark, AfterCount, AfterProcessingTime
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.runners import DataflowRunner, DirectRunner

def current_cart():
	url = "https://mercury.vtexcommercestable.com.br/api/checkout/pub/orderForm/"
	headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers)
	FJson = json.loads(response.text)
	result = json.dumps(FJson)
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/checkout_api/SHOPPING_CART/SHOPPING_CART.json", "w")
	text_file.write(result)
	text_file.close()
	cargando_bigquery()

def cargando_bigquery():
    print("Cargando a BigQuery")
    client = bigquery.Client()
    filename = '/home/bred_valenzuela/full_vtex/vtex/checkout_api/SHOPPING_CART/SHOPPING_CART.json'
    dataset_id = 'landing_zone'
    table_id = 'shopstar_vtex_current_cart'
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True
    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            location="southamerica-east1",
            job_config=job_config,)
        job.result()
	print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
	print("finalizado")


for x in range(1):
	current_cart()


class CommonLog(typing.NamedTuple):
    ip: str
    user_id: str
    lat: float
    lng: float
    timestamp: str
    http_request: str
    http_response: int
    num_bytes: int
    user_agent: str

beam.coders.registry.register_coder(CommonLog, beam.coders.RowCoder)

class ConvertToCommonLogFn(beam.DoFn):
  def process(self, element):
    try:
        row = json.loads(element.decode('utf-8'))
        yield beam.pvalue.TaggedOutput('parsed_row', CommonLog(**row))
    except:
        yield beam.pvalue.TaggedOutput('unparsed_row', element.decode('utf-8'))


class GetTimestampFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime().strftime("%Y-%m-%dT%H:%M:%S")
        output = {'page_views': element, 'timestamp': window_start}
        yield output

# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json from Pub/Sub into BigQuery')

    # Google Cloud options
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--staging_location', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--temp_location', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')

    # Pipeline-specific options
    parser.add_argument('--window_duration', required=True, help='Window duration in seconds')
    parser.add_argument('--table_name', required=True, help='Output BQ table')
    parser.add_argument('--input_topic', required=True, help='Input Pub/Sub topic')
    parser.add_argument('--allowed_lateness', required=True, help='Allowed lateness')
    parser.add_argument('--dead_letter_bucket', required=True, help='GCS Bucket for unparsable Pub/Sub messages')

    opts, pipeline_opts = parser.parse_known_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(pipeline_opts, save_main_session=True, streaming=True)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.staging_location
    options.view_as(GoogleCloudOptions).temp_location = opts.temp_location
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('streaming-minute-traffic-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    input_topic = opts.input_topic
    table_name = opts.table_name
    window_duration = opts.window_duration
    allowed_lateness = opts.allowed_lateness
    dead_letter_bucket = opts.dead_letter_bucket

    output_path = dead_letter_bucket + '/deadletter/'

    # Table schema for BigQuery
    table_schema = {
        "fields": [
            {
                "name": "page_views",
                "type": "INTEGER"
            },
            {
                "name": "timestamp",
                "type": "STRING"
            },

        ]
    }

    # Create the pipeline
    p = beam.Pipeline(options=options)



    rows = (p | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(input_topic)
              | 'ParseJson' >> beam.ParDo(ConvertToCommonLogFn()).with_outputs('parsed_row', 'unparsed_row')
                                                                 .with_output_types(CommonLog))

    (rows.unparsed_row
        | 'BatchOver10s' >> beam.WindowInto(beam.window.FixedWindows(60),
                                            trigger=AfterProcessingTime(60),
                                            accumulation_mode=AccumulationMode.DISCARDING)
        | 'WriteUnparsedToGCS' >> fileio.WriteToFiles(output_path,
                                                      shards=1,
                                                      max_writers_per_bundle=0)
        )

    (rows.parsed_row
        | "WindowByMinute" >> beam.WindowInto(beam.window.FixedWindows(int(window_duration)),
                                              trigger=AfterWatermark(late=AfterCount(1)),
                                              allowed_lateness=int(allowed_lateness),
                                              accumulation_mode=AccumulationMode.ACCUMULATING)
        | "CountPerMinute" >> beam.CombineGlobally(CountCombineFn()).without_defaults()
        | "AddWindowTimestamp" >> beam.ParDo(GetTimestampFn())
        | 'WriteAggToBQ' >> beam.io.WriteToBigQuery(
            table_name,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()

if __name__ == '__main__':
  run()
