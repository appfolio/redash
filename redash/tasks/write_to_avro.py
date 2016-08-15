import json
import collections
import datetime
from datetime import datetime
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import boto3

def write_to_avro(destination, event_stream_name, data):
    data = json.loads(data)
    schema_dic = collections.OrderedDict()
    schema_dic['name'] = event_stream_name
    schema_dic['type'] = "record"
    schema_dic['fields'] = []
    datetime_columns = []
    for column in data['columns']:
        dic = collections.OrderedDict()
        dic['name'] = column['name']
        dic['type'] = redash_type_to_avro_type(column['type'])
        if column['type'] == 'datetime':
            dic['logicalType'] = ['datetime', 'null']
            datetime_columns.append(column['name'])
        elif column['type'] == 'date':
            dic['logicalType'] =  ['date', 'null']
        schema_dic['fields'].append(dic)

    # Add query_created_date to the fields
    dic = collections.OrderedDict()
    dic['name'] = 'query_created_date'
    dic['type'] = ['string', 'null']
    dic['logicalType'] = ['date', 'null']
    schema_dic['fields'].append(dic)

    with open('temp.avsc', 'w') as outfile:
        json.dump(schema_dic, outfile)

    schema = avro.schema.parse(open('temp.avsc', 'rb').read())

    current_datetime = str(datetime.now())
    current_date = current_datetime.split(' ')
    writer = DataFileWriter(open('temp.avro', 'wb'), DatumWriter(), schema)
    for user in data['rows']:
        for name, content in user.iteritems():
            if name in datetime_columns:
                if content is not None:
                    user[name] = content.replace('T', ' ')
        user['query_created_date'] = current_date[0]
        writer.append(user)
    writer.close()

    sub_director = current_date[0].replace('-', '/')
    schema_file_name = event_stream_name + '-' + current_datetime + '.avsc'
    data_file_name = event_stream_name + '-' + current_datetime + '.avro'

    s3 = boto3.resource('s3')
    s3.Object('appfolio-kinesis-test', destination + '/' + sub_director + '/'+ 'schema/' + event_stream_name + '/' + schema_file_name).put(Body=open('temp.avsc', 'rb'))
    s3.Object('appfolio-kinesis-test', destination + '/' + sub_director + '/'+ 'data/' + event_stream_name + '/' + data_file_name).put(Body=open('temp.avro', 'rb'))
    s3.Object('appfolio-kinesis-test', 'avro_data_history/' + destination + '/data/' + event_stream_name + '/' + data_file_name).put(Body=open('temp.avro', 'rb'))


def redash_type_to_avro_type(column_type):
    if column_type is None:
      return ['string', 'null']
    elif column_type == 'integer':
      return ['int', 'null']
    elif column_type == 'datetime':
      return ['string', 'null']
    elif column_type == 'date':
      return ['string', 'null']
    else:
      return [column_type, 'null']


