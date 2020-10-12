import boto3
import botocore
import json
from io import StringIO
import pandas as pd
import logging
import time
import os


_BUCKET = 'real-estate-scrapping'
_PATH = 'holmes_weekly_detailed/'


def save_to_s3(data, key):
    logging.debug('Saving to: s3://{}/{}'
                  .format(_BUCKET, key))

    buf = StringIO()
    data.to_csv(buf, sep='\t', index=False)
    session = boto3.session.Session(profile_name="aero")
    s3 = session.resource("s3")
    s3.Object(_BUCKET, key).put(Body=buf.getvalue())

    print('File saved as s3://{}/{}'.format(_BUCKET, key))

    return 's3://{}/{}'.format(_BUCKET, key)


files = os.listdir('output/')
fs = [(f, '2020-' + f[f.find('_')+1:f.find('.tsv')][:2] + '-' + f[f.find('_')+1:f.find('.tsv')][2:]) for f in files if f.startswith('holmes.bg')]

for f, date in fs:
    d = pd.read_csv('output/' + f, sep='\t')
    filename = date + '/holmes_' + date + '.tsv'
    save_to_s3(d, _PATH + filename)



