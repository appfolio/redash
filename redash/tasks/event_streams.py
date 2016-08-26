import boto3
import time
import json
import datetime
from celery.utils.log import get_task_logger
from redash.worker import celery
from redash import models
from .base import BaseTask

firehose = boto3.client('firehose')
logger = get_task_logger(__name__)

FIREHOSE_BATCH_SIZE_LIMIT = 500

@celery.task(name="redash.tasks.write_to_firehose", base=BaseTask)
def write_to_firehose(query_result_id, stream_name, query_name):
    query_result = models.QueryResult.get_by_id(query_result_id)
    ensure_delivery_stream_created(stream_name)
    write_batch_data_to_s3(stream_name, query_result.data, query_name)

    logger.info("Results of query '{}' written to event stream '{}'".format(query_name, stream_name))
    return query_result_id


def event_stream_callback_for(query_id):
    query = models.Query.get_by_id(query_id)

    if query.options.get('save_scheduled_result_to_s3_and_redshift', False):
        return write_to_firehose.s('data_to_both_s3_and_redshift', query.name)
    elif query.options.get('save_scheduled_result_to_s3', False):
        return write_to_firehose.s('data_to_s3', query.name)
    else:
        return None


def ensure_delivery_stream_created(delivery_stream_name):
    if delivery_stream_name not in [name for name in firehose.list_delivery_streams()['DeliveryStreamNames']]:
        logger.info('Creating Firehose delivery stream %s' % delivery_stream_name)
        firehose.create_delivery_stream(
            DeliveryStreamName=delivery_stream_name,
            S3DestinationConfiguration={
                'RoleARN':  'arn:aws:iam::088684165182:role/firehose_delivery_role',
                'BucketARN': 'arn:aws:s3:::appfolio-kinesis-test',
                'Prefix': delivery_stream_name + '/',
                'BufferingHints': {
                    'SizeInMBs': 100,
                    'IntervalInSeconds': 60
                }
            }
        )

    while firehose.describe_delivery_stream(DeliveryStreamName=delivery_stream_name)['DeliveryStreamDescription']['DeliveryStreamStatus'] == 'CREATING':
        time.sleep(2)


def write_batch_data_to_s3(delivery_stream_name, data, event_stream):
    if delivery_stream_name in firehose.list_delivery_streams()['DeliveryStreamNames']:
        desc = firehose.describe_delivery_stream(DeliveryStreamName=delivery_stream_name)
        if desc['DeliveryStreamDescription']['DeliveryStreamStatus'] == 'ACTIVE':
            for record_batch in parse_data_to_batches(data, event_stream):
                firehose.put_record_batch(DeliveryStreamName=delivery_stream_name, Records=record_batch)
        else:
            logger.info('Waiting for AWS to finish creating your delivery_stream')
    else:
        logger.error('The stream %s does not exist' % delivery_stream_name)


def parse_data_to_batches(query_data, event_name):
    query_data = json.loads(query_data)
    batch_header_event = format_event({ 'event_name': event_name, 'date': str(datetime.datetime.now().date()), 'columns': query_data['columns'] })

    batch_list = []
    for i, row in enumerate(query_data['rows']):
        if i % (FIREHOSE_BATCH_SIZE_LIMIT - 1) == 0:
            current_batch = [batch_header_event]
            batch_list.append(current_batch)
        current_batch.append(format_event({ 'row': row }))

    return batch_list


def format_event(event_data):
    return { 'Data': str(json.dumps(event_data)) }

