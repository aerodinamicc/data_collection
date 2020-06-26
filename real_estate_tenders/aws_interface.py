import boto3
import botocore
import json
from io import StringIO
import pandas as pd
import logging
import time


with open('config.json') as json_file:
    config = json.load(json_file)
    _S3_PREFIX = config['prefix']
    _S3_BUCKET = config['bucket']
    _S3_PATH = config['path']
    _DATABASE = config['database']
    _PROFILE = config['profile']

_SESSION = boto3.Session(profile_name=_PROFILE)
_S3_CLIENT = _SESSION.client('s3')
_S3_RESOURCE = _SESSION.resource('s3')
_ATHENA_CLIENT = _SESSION.client('athena')


def query_athena(query, bucket=_S3_BUCKET, path=_S3_PATH):
    sent_query = 'The query sent to Athena: \n{}'.format(query)
    logging.debug(sent_query)
    logging.debug('Waiting till the query is completed.')

    response = _ATHENA_CLIENT.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': _DATABASE
        },
        ResultConfiguration={
            'OutputLocation': _S3_PREFIX + bucket + "/" + path,
        },
        WorkGroup='dp2-power-analysts'
    )

    execution_id = response['QueryExecutionId']
    state = 'RUNNING'

    while state == 'RUNNING':
        response = _ATHENA_CLIENT.get_query_execution(QueryExecutionId=execution_id)

        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if state == 'FAILED':
                message = 'Query failed. Via Athena for more details.'
                logging.debug(message)
            elif state == 'SUCCEEDED':
                time.sleep(5)
                logging.debug('Query has been successfully processed.')
                return execution_id + '.csv'

        time.sleep(1)


def fetch_from_s3(s3_object, bucket=_S3_BUCKET, path=_S3_PATH):
    try:
        with open(s3_object, 'wb') as data:
            _S3_CLIENT.download_fileobj(bucket, path + s3_object, data)
        logging.debug('Downloaded s3://{}/{}'.format(bucket, path + s3_object))

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logging.debug("The object {} does not exist.".format(bucket + path + s3_object))
            s3_object = None

    return s3_object


def fetch_from_s3_exp():
    s3 = boto3.resource('s3')
    obj = s3.Object(bucketname, itemname)
    body = obj.get()['Body'].read()


def save_to_s3(data, s3_object, bucket=_S3_BUCKET, path=_S3_PATH):
    key = path + s3_object
    logging.debug('Saving to: s3://{}/{}'
                  .format(bucket, key))

    buf = StringIO()
    data.to_csv(buf, index=False)
    _S3_RESOURCE.Object(bucket, key).put(Body=buf.getvalue())

    logging.debug('File saved as s3://{}/{}'.format(bucket, key))

    return 's3://{}/{}'.format(bucket, key)
