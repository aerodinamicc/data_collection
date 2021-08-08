import yfinance as yf
import pandas as pd
import numpy as np
import psycopg2
from io import StringIO
import os
import yaml
from tqdm import tqdm
import json


class Pipeline():
    def __init__(self):
        self._WORKING_DIRECTORY = os.path.dirname(os.path.realpath(__file__))

        with open(os.path.join(self._WORKING_DIRECTORY, 'config.yaml')) as file:
            self._CONFIG = yaml.full_load(file)

    def copy_to_db(self, conn, df, table):
        with conn.cursor() as cursor:
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            try:
                cursor.copy_from(buffer, table, null="", sep=',')
                conn.commit()
            except Exception as error:
                print("Error: {}".format(error)) 
                conn.rollback()

    def deploy_schema(self, conn):
        with conn.cursor() as cursor:
            with open(os.path.join(self._WORKING_DIRECTORY, self._CONFIG['db_schema'])) as schema:
                cursor.execute(schema.read())
                conn.commit()

    def run(self):
        sp = pd.read_csv(self._CONFIG['sp500_dataset'])
        sp['details'] = ''

        with psycopg2.connect(host=self._CONFIG['db_host'], \
                                port=self._CONFIG['db_port'], \
                                database=self._CONFIG['db_name'], \
                                user=self._CONFIG['db_user'], \
                                password=self._CONFIG['db_pass']) as conn:
            #self.deploy_schema(conn)
            #self.copy_to_db(conn, sp, self._CONFIG['companies_table'])

            syms = sp[self._CONFIG['symbol_column']].values

            with conn.cursor() as cursor:
                q = "select symbol from sp500_companies sc where details is not null;"
                cursor.execute(q)
                symbols_in_db = pd.DataFrame(cursor.fetchall(), columns=['symbol'])
        
            for sym in tqdm(syms):
                if sym in list(symbols_in_db.symbol):
                    continue
                data = yf.download(tickers=sym.strip(), period='2y', interval='1d', progress=False)
                data['symbol'] = sym
                data['at_date'] = data.index
                data.rename(columns={'Open': 'at_open',
                                    'High': 'high',
                                    'Low': 'low',
                                    'Close': 'at_close',
                                    'Adj Close': 'at_close_adj',
                                    'Volume': 'volume'}, inplace=True)
                
                data = data[self._CONFIG['list_of_ordered_columns']]

                self.copy_to_db(conn, data, self._CONFIG['price_actions_table'])

                ticker_info = yf.Ticker(sym).info
                if 'longBusinessSummary' in ticker_info.keys():
                    del ticker_info['longBusinessSummary']

                if 'shortName' in ticker_info.keys():
                    del ticker_info['shortName']

                if 'longName' in ticker_info.keys():
                    del ticker_info['longName']

                #import pdb; pdb.set_trace()

                details_query = "UPDATE {} SET details = '{}' WHERE symbol = '{}'".format(self._CONFIG['companies_table'], json.dumps(ticker_info), sym)

                with conn.cursor() as cursor:
                    cursor.execute(details_query)
                    conn.commit()



if __name__ == '__main__':
    pipe = Pipeline()
    pipe.run()



