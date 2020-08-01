import argparse
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


def main():
    links = pd.read_csv('etuovi_links.csv')['link'].values
    offers = pd.DataFrame()

    #with open('C:/Users/shadow/Downloads/etuovi_test.html', 'r', encoding='utf8') as f:
    #    file = f.read()

    for l in tqdm(links):
        resp = requests.get(l)

        page = bs4.BeautifulSoup(resp.text, 'lxml')
        keys = page.findAll('div', attrs={'class': 'ItemHeader__itemHeader__32xAv'})
        values = page.findAll('div', attrs={'class': 'CompactInfoRow__content__3jGt4'})

        details = {}

        for i in range(len(keys)):
            if len(values[i].findAll('ul')) > 0:
                resp_value = clean_text(' '.join([li.text for li in values[i].find('ul').findAll('li')]))
            else:
                resp_value = clean_text(values[i].text.strip())

            details[keys[i].text.strip()] = resp_value

        selling_price = convert_price(details['Myyntihinta']) if 'Myyntihinta' in details.keys() else ''
        debt_component = convert_price(details['Velkaosuus']) if 'Velkaosuus' in details.keys() else ''
        total_price = convert_price(re.search('^([\d]+)', convert_price(details['Velaton hinta'])).group(1)) \
                        if 'Velaton hinta' in details.keys() \
                        and re.search('^([\d,]+)', details['Velaton hinta']) is not None \
                        else ''
        total_monthly_fee = details['Yhtiövastike'] if 'Yhtiövastike' in details.keys() else ''
        monthly_fee = re.search('^([\d,\s]+)', total_monthly_fee).group(1).replace(',', '.').replace(' ', '') if  re.search('^([\d,\s]+)', total_monthly_fee) is not None else ''
        maintainance_fee = re.search('Hoitovastike ([\d,\s]+)', total_monthly_fee).group(1).replace(',', '.').replace(' ', '') if  re.search('Hoitovastike ([\d,\s]+)', total_monthly_fee) is not None else ''
        financial_fee = re.search('Rahoitusvastike ([\d,\s]+)', total_monthly_fee).group(1).replace(',', '.').replace(' ', '') if  re.search('Rahoitusvastike ([\d,\s]+)', total_monthly_fee) is not None else ''  
        floor = details['Kerrokset'] if 'Kerrokset' in details.keys() else ''
        communications = details['Liikenneyhteydet'] if 'Liikenneyhteydet' in details.keys() else ''

        offers = offers.append({'link': l,
                                'total_price': total_price,
                                'selling_price': selling_price,
                                'debt_component': debt_component,
                                'total_monthly_fee': monthly_fee,
                                'maintainance_fee': maintainance_fee,
                                'financial_fee': financial_fee,
                                'floor': floor,
                                'communications': communications,
                                'details': str(details)}, ignore_index=True)
        
    offers.to_csv('etuovi_details.tsv', sep='\t', index=False)


if __name__ == '__main__':
	main()