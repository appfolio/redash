import boto3
import time
import json
import datetime
from celery.utils.log import get_task_logger
from redash.worker import celery
from redash import models, settings

firehose = boto3.client('firehose')
logger = get_task_logger(__name__)

FIREHOSE_BATCH_SIZE_LIMIT = 500

@celery.task(name="redash.tasks.write_to_firehose")
def write_to_firehose(query_result_id, schema_stream_name, data_stream_name, query_name):
    query_result = models.QueryResult.get_by_id(query_result_id)
    result_data = json.loads(query_result.data)

    columns = result_data['columns']
    columns.append({ 'type': 'datetime', 'friendly_name': 'occurred_at', 'name': 'occurred_at' }) # TODO: what if there is already an occurred_at column?
    schema_event = format_event({ 'event_name': query_name, 'columns': columns })
    firehose.put_record(DeliveryStreamName=schema_stream_name, Record=schema_event)

    batch = []
    for i, row in enumerate(result_data['rows']):
        if i % FIREHOSE_BATCH_SIZE_LIMIT == 0 and i != 0:
            firehose.put_record_batch(DeliveryStreamName=data_stream_name, Records=batch)
            batch = []
        row['occurred_at'] = str(query_result.retrieved_at)
        batch.append(format_event({ 'event_name': query_name, 'row': row }))

    firehose.put_record_batch(DeliveryStreamName=data_stream_name, Records=batch)

    logger.info("Results of query '{}' written to event stream '{}'".format(query_name, data_stream_name))
    return query_result_id


def event_stream_callback_for(query_id):
    query = models.Query.get_by_id(query_id)

    if query.options.get('save_scheduled_result_to_s3_and_redshift', False):
        return write_to_firehose.s(settings.S3_AND_REDSHIFT_SCHEMA_STREAM, settings.S3_AND_REDSHIFT_DATA_STREAM, query.name)
    elif query.options.get('save_scheduled_result_to_s3', False):
        return write_to_firehose.s(settings.S3_ONLY_SCHEMA_STREAM, settings.S3_ONLY_DATA_STREAM, query.name)
    else:
        return None


def format_event(event_data):
    return { 'Data': str(json.dumps(event_data)) }

