from celery.utils.log import get_task_logger
from redash.worker import celery
from redash import models
from .base import BaseTask
from redash.tasks.firehose_to_s3 import create_delivery_stream, write_batch_data_to_s3

logger = get_task_logger(__name__)


@celery.task(name="redash.tasks.write_to_firehose", base=BaseTask)
def write_to_firehose(query_result_id, stream_name, query_name):
    query_result = models.QueryResult.get_by_id(query_result_id)
    create_delivery_stream(stream_name, stream_name) # TODO: what should the prefix be?
    write_batch_data_to_s3(stream_name, query_result.data, query_name)

    logger.info("Results of query '{}' written to event stream '{}'".format(query_name, stream_name))
    return query_result_id


def event_stream_callback_for(query_id):
    query = models.Query.get_by_id(query_id)

    if query.send_to_redshift_and_s3:
        write_to_firehose.s('data_to_both_s3_and_redshift', query.name)
    elif query.send_to_s3:
        write_to_firehose.s('data_to_s3', query.name)
    else:
        None


