import boto3
import botocore
import json
from io import StringIO
import pandas as pd
import logging
import time


def query_athena(athena_client, query, bucket, path):
    sent_query = 'The query sent to Athena: \n{}'.format(query)
    logging.debug(sent_query)
    logging.debug('Waiting till the query is completed.')

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': "real_estate_db"
        },
        ResultConfiguration={
            'OutputLocation': "s3://" + bucket + '/' + path,
        },
        WorkGroup='primary'
    )

    execution_id = response['QueryExecutionId']
    state = 'RUNNING'

    while state in ('RUNNING', 'QUEUED'):
        response = athena_client.get_query_execution(QueryExecutionId=execution_id)

        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if state in ('FAILED', 'CANCELLED'):
                message = 'Query failed. Via Athena for more details.'
                logging.debug(message)
            elif state == 'SUCCEEDED':
                time.sleep(5)
                logging.debug('Query has been successfully processed.')
                return execution_id + '.csv'

        time.sleep(1)


def fetch_from_s3(s3_client, s3_object, bucket, path):
    print('Looking for: ' + path + s3_object)
    obj = s3_client.get_object(Bucket= bucket, Key= path + s3_object) 
    d = pd.read_csv(obj['Body'], dtype=str)

    return d


def save_to_s3(s3_resource, data, s3_object, bucket, path):
    key = path + s3_object
    logging.debug('Saving to: s3://{}/{}'.format(bucket, key))

    buf = StringIO()
    data.to_csv(buf, index=False)
    s3_resource.Object(bucket, key).put(Body=buf.getvalue())

    logging.debug('File saved as s3://{}/{}'.format(bucket, key))

    return 's3://{}/{}'.format(bucket, key)
