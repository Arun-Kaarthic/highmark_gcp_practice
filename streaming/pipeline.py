"""Python Apache Beam: CPR Kafka CDC -> windowed JSONL in GCS Raw.

Requires a KafkaIO expansion service (Java) for Beam's cross-language Kafka connector.
The Flex Template container installs Java 17 for the default expansion service.
"""
import argparse
import json
import logging
import re

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import fileio
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows


class ParseCdc(beam.DoFn):
    def process(self, record):
        # ReadFromKafka produces (key, value) byte pairs.
        key, value = record
        try:
            event = json.loads(value.decode('utf-8'))
            if not isinstance(event, dict):
                raise ValueError('CDC payload must be a JSON object')
            operation = str(event.get('operation', '')).upper()
            table = str(event.get('table', ''))
            if operation not in {'INSERT', 'UPDATE', 'DELETE'}:
                raise ValueError('Invalid or missing operation')
            if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', table):
                raise ValueError('Invalid or missing table')
            event['_kafka_key'] = key.decode('utf-8', errors='replace') if key else None
            yield json.dumps(event, ensure_ascii=False, separators=(',', ':'))
        except Exception as exc:
            logging.warning('Rejected Kafka message: %s', exc)
            yield pvalue.TaggedOutput('invalid', json.dumps({
                'error': str(exc),
                'raw_value': value.decode('utf-8', errors='replace')[:10000],
            }, ensure_ascii=False))


def file_name(window, pane, shard_index, total_shards, compression, destination):
    # Beam controls unique shards within each window; downstream must tolerate retries.
    start = window.start.to_utc_datetime().strftime('%Y%m%dT%H%M%S')
    return (f'{destination}/{start}-pane{pane.index}-'
            f'{shard_index:05d}-of-{total_shards:05d}.jsonl')


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--kafka_bootstrap', required=True)
    parser.add_argument('--kafka_topic', required=True)
    parser.add_argument('--kafka_group', default='cpr-dataflow-raw')
    parser.add_argument('--raw_prefix', required=True, help='gs://bucket/cdc')
    parser.add_argument('--window_seconds', type=int, default=60)
    parser.add_argument('--kafka_security_protocol', default='PLAINTEXT')
    parser.add_argument('--kafka_sasl_mechanism', default='PLAIN')
    parser.add_argument('--kafka_username', default='')
    parser.add_argument('--kafka_password', default='')
    known, pipeline_args = parser.parse_known_args(argv)

    consumer_config = {
        'bootstrap.servers': known.kafka_bootstrap,
        'group.id': known.kafka_group,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': 'false',
        'security.protocol': known.kafka_security_protocol,
    }
    if known.kafka_security_protocol.startswith('SASL'):
        if not known.kafka_username or not known.kafka_password:
            raise ValueError('SASL requires Kafka username and password')
        consumer_config['sasl.mechanism'] = known.kafka_sasl_mechanism
        consumer_config['sasl.jaas.config'] = (
            'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{known.kafka_username}" password="{known.kafka_password}";'
        )

    options = PipelineOptions(pipeline_args, save_main_session=True)
    options.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=options) as pipeline:
        parsed = (
            pipeline
            | 'KafkaCDC' >> ReadFromKafka(
                consumer_config=consumer_config,
                topics=[known.kafka_topic],
            )
            | 'ParseCDC' >> beam.ParDo(ParseCdc()).with_outputs('invalid', main='valid')
        )
        valid = parsed.valid | 'WindowValid' >> beam.WindowInto(FixedWindows(known.window_seconds))
        invalid = parsed.invalid | 'WindowInvalid' >> beam.WindowInto(FixedWindows(known.window_seconds))

        _ = valid | 'WriteRawCDC' >> fileio.WriteToFiles(
            path=known.raw_prefix.rstrip('/'),
            destination=lambda line: json.loads(line)['table'],
            sink=lambda dest: fileio.TextSink(),
            file_naming=file_name,
            shards=1,
        )
        _ = invalid | 'WriteInvalidCDC' >> fileio.WriteToFiles(
            path=known.raw_prefix.rstrip('/') + '/_deadletter',
            destination=lambda _: 'invalid',
            sink=lambda dest: fileio.TextSink(),
            file_naming=file_name,
            shards=1,
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
