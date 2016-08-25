import boto3
import time
import json
import datetime

firehose = boto3.client('firehose')

def create_delivery_stream(delivery_stream_name, prefix):
    if delivery_stream_name not in [name for name in firehose.list_delivery_streams()['DeliveryStreamNames']]:
        print 'Creating Firehose delivery stream %s' % delivery_stream_name
        firehose.create_delivery_stream(
            DeliveryStreamName=delivery_stream_name,
            S3DestinationConfiguration={
                'RoleARN':  'arn:aws:iam::088684165182:role/firehose_delivery_role',
                'BucketARN': 'arn:aws:s3:::appfolio-kinesis-test',
                'Prefix': prefix + '/',
                'BufferingHints': {
                    'SizeInMBs': 100,
                    'IntervalInSeconds': 60
                }
            }
        )
    else:
        print 'Firehose delivery Stream %s exists' % (delivery_stream_name)

    while firehose.describe_delivery_stream(DeliveryStreamName=delivery_stream_name)['DeliveryStreamDescription']['DeliveryStreamStatus'] == 'CREATING':
        time.sleep(2)

    return firehose.describe_delivery_stream(DeliveryStreamName=delivery_stream_name) ['DeliveryStreamDescription']

def write_batch_data_to_s3(delivery_stream_name, data, event_stream):   
    desc = firehose.describe_delivery_stream(
        DeliveryStreamName=delivery_stream_name
    )

    batch_list = parse_data(data, event_stream)
    result = []

    if delivery_stream_name in [name for name in firehose.list_delivery_streams()['DeliveryStreamNames']]:
        if desc['DeliveryStreamDescription']['DeliveryStreamStatus'] == 'ACTIVE':
            for records in batch_list:
                result = firehose.put_record_batch(
                    DeliveryStreamName=delivery_stream_name,
                    Records= records
                )
        else:
            print 'Wait for AWS finishing creating your delivery_stream'
    else:
        print 'No %s firehose exists!' % delivery_stream_name

    return result


def parse_data(query_data, event_name):
    query_data = json.loads(query_data)
    rows = query_data['rows']
    columns = query_data['columns']
    current_time = datetime.datetime.now()
    date = current_time.date() 

    batch_list = list()
    if len(rows) != 0:
        for i in range(0, len(rows)):
            if i % 499 == 0:
                if i != 0 :
                    batch_list.append(result)
                result = []
                temp = dict()
                data = dict()
                data['event_name'] = event_name
                data['date'] = str(date)
                data['columns'] = columns
                data = json.dumps(data)
                temp['Data'] = str(data)
                result.append(temp)
            temp = dict()
            data = dict()
            data['row'] = rows[i]
            data = json.dumps(data)
            temp['Data'] = str(data)
            result.append(temp)
        if len(result) > 1:
            batch_list.append(result)

    return batch_list