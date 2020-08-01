import argparse
import ast
import bs4
import requests
import pandas as pd
import re
import os
from tqdm import tqdm
import json
import urllib
from helpers import clean_text, replace_month_with_digit, months
from datetime import datetime

 # Myyntihinta - selling price
 # Velkaosuus - debt_component
 # Yhtiövastike - total monthly fee
 # Rahoitusvastike - financial fee
 # Hoitovastike - maintainance fee
 # Kerrokset - floor
 # Sauna
 # Liikenneyhteydet - communications

 # value - CompactInfoRow__content__3jGt4
 # key - ItemHeader__itemHeader__32xAv

def convert_price(price_str):
    return price_str.replace(' €', ''). replace(' ', '').replace(',', '.')


def clean_with_regex(string):
    return convert_price(re.search('^([\d\s,]+)', string).group(1)) if re.search('^([\d\s,]+)', string) is not None else ''


def main():
    df = pd.read_csv('etuovi_details.tsv', sep='\t')[['link', 'details']]
    df['details'] = df['details'].map(ast.literal_eval)

    #import pdb; pdb.set_trace()
    df['selling_price'] = df['details'].apply(lambda x: convert_price(x['Myyntihinta']) if 'Myyntihinta' in x.keys() else '')
    df['debt_component'] = df['details'].apply(lambda x: convert_price(x['Velkaosuus']) if 'Velkaosuus' in x.keys() else '')
    df['total_price'] = df['details'].apply(lambda x: convert_price(re.search('^([\d\s,]+)', x['Velaton hinta']).group(1)) \
                            if 'Velaton hinta' in x.keys() \
                            and re.search('^([\d\s,]+)', x['Velaton hinta']) is not None \
                            else '')
    df['total_monthly_fee'] = df['details'].apply(lambda x: x['Yhtiövastike'] if 'Yhtiövastike' in x.keys() else '')
    df['monthly_fee'] = df['total_monthly_fee'].apply(lambda x: clean_with_regex(x))
    df['maintainance_fee'] = df['total_monthly_fee'].apply(lambda x: convert_price(re.search('Hoitovastike ([\d,\s]+)', x).group(1)) if  re.search('Hoitovastike ([\d,\s]+)', x) is not None else '')
    df['financial_fee'] = df['total_monthly_fee'].apply(lambda x: convert_price(re.search('Rahoitusvastike ([\d,\s]+)', x).group(1)) if  re.search('Rahoitusvastike ([\d,\s]+)', x) is not None else '')
    df['floor'] = df['details'].apply(lambda x: x['Kerrokset'] if 'Kerrokset' in x.keys() else '')
    df['communications'] = df['details'].apply(lambda x: x['Liikenneyhteydet'] if 'Liikenneyhteydet' in x.keys() else '')

    #import pdb; pdb.set_trace()
        
    df.to_csv('etuovi_details_0.1.tsv', sep='\t', index=False)


if __name__ == '__main__':
	main()