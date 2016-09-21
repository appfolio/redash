import boto3
import time
import json
import datetime
from celery.utils.log import get_task_logger
from redash.worker import celery
from redash import models, settings
from .base import BaseTask

firehose = boto3.client('firehose')
logger = get_task_logger(__name__)

FIREHOSE_BATCH_SIZE_LIMIT = 500

@celery.task(name="redash.tasks.write_to_firehose", base=BaseTask)
def write_to_firehose(query_result_id, stream_name, query_name):
    query_result = models.QueryResult.get_by_id(query_result_id)
    write_batch_data_to_s3(stream_name, query_result.data, query_name)

    logger.info("Results of query '{}' written to event stream '{}'".format(query_name, stream_name))
    return query_result_id


def event_stream_callback_for(query_id):
    query = models.Query.get_by_id(query_id)

    if query.options.get('save_scheduled_result_to_s3_and_redshift', False):
        return write_to_firehose.s(settings.S3_AND_REDSHIFT_STREAM, query.name)
    elif query.options.get('save_scheduled_result_to_s3', False):
        return write_to_firehose.s(settings.S3_ONLY_STREAM, query.name)
    else:
        return None


def write_batch_data_to_s3(stream_name, data, event_name):
    for record_batch in parse_data_to_batches(data, event_name):
        firehose.put_record_batch(DeliveryStreamName=stream_name, Records=record_batch)


def parse_data_to_batches(query_data, event_name):
    query_data = json.loads(query_data)
    batch_header_event = format_event({ 'event_name': event_name, 'occurred_at': str(datetime.datetime.now()), 'columns': query_data['columns'] })

    batch_list = []
    for i, row in enumerate(query_data['rows']):
        if i % (FIREHOSE_BATCH_SIZE_LIMIT - 1) == 0:
            current_batch = [batch_header_event]
            batch_list.append(current_batch)
        current_batch.append(format_event({ 'row': row }))

    return batch_list


def format_event(event_data):
    return { 'Data': str(json.dumps(event_data)) }

