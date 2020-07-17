#!/usr/bin/env python
# -*- coding: utf-8 -*-
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


sale_url = 'http://sofia.holmes.bg'
rent_url = 'http://naemi-sofia.holmes.bg/'
offers_daily = "holmes_daily_"


def get_neighbourhood_links(url):
    search_page_template = '/pcgi/home.cgi?act=5&f1=0&f2=1&f3=0&f4=%E3%F0%E0%E4%20%D1%EE%F4%E8%FF'
    # page_link = base_url + search_page_template
    resp = requests.get(url)
    page = bs4.BeautifulSoup(resp.content.decode('cp1251'), 'html')
    neighbourhoods = page.findAll('a',
                                  href=re.compile('sofia\.holmes\.bg/pcgi/home\.cgi.*f4=град(?:\s)?София.*f5=[А-Яа-я0-9\s]+'),
                                  # text=re.compile('[\d]+'),
                                  attrs={'class': 'linkSearch'})
    neighbourhoods = [re.search('&f5=(.*)$', i['href']).group(1) for i in neighbourhoods]
    neighbourhoods = list(set([(n, url + search_page_template + '&f5=' + urllib.parse.quote(n.encode('cp1251'))) for n in neighbourhoods]))
    return neighbourhoods


def get_all_search_pages(neighbourhoods):
    all_search_pages = []
    for n in tqdm(neighbourhoods):
        pages = set([1])
        last_page = False
        page_link = n[1]

        while not last_page:
            resp = requests.get(page_link)
            page = bs4.BeautifulSoup(resp.content.decode('cp1251'), 'html')

            visible_pages = page.findAll('a', href=re.compile('sofia\.holmes\.bg/pcgi/home\.cgi.*f6='), text=re.compile('[\d]+'), attrs={'class': 'pageNumbers'})
            visible_pages = [int(i.text) for i in visible_pages]

            next_page = page.findAll('a', href=re.compile('sofia\.holmes\.bg/pcgi/home\.cgi.*f6='), text=re.compile('^(?![\d]+)'), attrs={'class': 'pageNumbers'})
            href = next_page[0]['href'] if len(next_page) > 0 else None
            page_number = int(re.search('([\d]+)$', href).group(1)) if href is not None else None

            if len(visible_pages) > 0 and len(next_page) > 0 and page_number > max(pages):
                pages.update(set([page_number]))
                page_link = n[1] + '&f6={}'.format(page_number)
            else:
                last_page = True

            pages.update(set(visible_pages))

        search_pages = [n[1] + '&f6={}'.format(p) for p in pages]
        all_search_pages = all_search_pages + search_pages

    return all_search_pages


def get_all_offers(search_pages):
    offers = pd.DataFrame()

    for p in tqdm(search_pages):
        #import pdb; pdb.set_trace()
        resp = requests.get(p)
        page = bs4.BeautifulSoup(resp.content.decode('cp1251'), 'html')

        boxes = page.findAll('table', attrs={'width':'956'})[3:23]
        
        for b in boxes:
            try:
                tds = b.findAll('td')
                link = tds[2].a['href'] if len(tds) > 2 else ''
                id = re.search('adv=(.*)$', link).group(1)
                place = tds[2].a.text.replace('град София,', '') if len(tds) > 2 else ''
                area = tds[5].text.replace(' кв.м', '') if len(tds) > 5 else ''
                price = tds[3].text.strip() if len(tds) > 3 else ''
                price_orig = tds[3].text.strip() if len(tds) > 3 else ''

                price = re.search('([\d\s]+)', price).group(1).replace(' ', '') if re.search('([\d\s]+)', price) else '0'
                if 'Цена при запитване' in price_orig:
                    price = '0'
                elif 'eur' in price_orig.lower():
                    currency = 'EUR'
                elif 'лв' in price_orig.lower():
                    price = str(round(float(price) / 1.9558)) if price != '0' else '0'
                    currency = 'BGN'
                
                if 'на кв.м' in price_orig:
                    #print('\n{} * {} = {}'.format(float(price), float(area), round(float(price) * float(area), 0)))
                    price = round(float(price) * float(area), 0)
                
                typ = tds[4].text if len(tds) > 4 else ''
                desc = tds[7].text if len(tds) > 7 else ''
                agency = tds[8].a['href'] if len(tds) > 8 and len(tds[8].findAll('a')) > 0 else ''

                offers = offers.append({'link': sale_url + link,
                                        'id': id,
                                        'type': typ,
                                        'place': place,
                                        'price': price,
                                        'area': area,
                                        'description': desc,
                                        'currency':currency,
                                        'agency': agency}, ignore_index=True)

            except Exception as e:
                #import pdb; set_trace()
                print(e)
                

    return offers


def gather_new_articles():
    sale_neighbourhoods = get_neighbourhood_links(sale_url)
    sale_search_pages = get_all_search_pages(sale_neighbourhoods)    
    sale_offers = get_all_offers(sale_search_pages)
    sale_offers['is_for_sale'] = True      

    return sale_offers


if __name__ == '__main__':
	gather_new_articles()