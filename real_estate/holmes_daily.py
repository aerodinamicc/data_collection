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
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options


sale_url = 'http://sofia.holmes.bg'
rent_url = 'http://naemi-sofia.holmes.bg/'
offers_daily = "holmes_daily_"


def get_neighbourhood_links(url):
    search_page_template = '/pcgi/home.cgi?act=5&f1=0&f2=1&f3=0&f4=%E3%F0%E0%E4%20%D1%EE%F4%E8%FF'
    # page_link = base_url + search_page_template
    resp = requests.get(url)
    page = bs4.BeautifulSoup(resp.content.decode('cp1251'), features='html.parser')
    neighbourhoods = page.findAll('a',
                                  href=re.compile('sofia\.holmes\.bg/pcgi/home\.cgi.*f4=град(?:\s)?София.*f5=[А-Яа-я0-9\s]+'),
                                  # text=re.compile('[\d]+'),
                                  # attrs={'class': 'linkSearch'}
                                  )
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
            page = bs4.BeautifulSoup(resp.content.decode('cp1251'), features='html.parser')

            visible_pages = page.findAll('a', href=re.compile('sofia\.holmes\.bg/pcgi/home\.cgi.*f6='), text=re.compile('[\d]+')) #, attrs={'class': 'pageNumbers'})
            visible_pages = [int(i.text) for i in visible_pages]

            next_page = page.findAll('a', href=re.compile('sofia\.holmes\.bg/pcgi/home\.cgi.*f6='), text=re.compile('^(?![\d]+)')) #, attrs={'class': 'pageNumbers'})
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

        #import pdb; pdb.set_trace()

    return all_search_pages


def get_place_and_labels(str):
    if str.find('(публикувана') > -1:
        return (str[:str.find('(публикувана')], 'published recently')
    
    return (str, '')


def get_all_offers(search_pages):
    offers = pd.DataFrame()
    options = Options()
    options.headless = True
    options.add_argument('log-level=3')
    browser = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    
    #import pdb; pdb.set_trace()
    for p in tqdm(search_pages):
        
        browser.get(p)
        page = bs4.BeautifulSoup(browser.page_source, features='html.parser')
        #resp = requests.get(p)
        #page = bs4.BeautifulSoup(resp.content.decode('cp1251'), 'html')



        boxes = page.find_all('div', attrs={'class': 'items'})[0].findAll('item')
        
        for b in boxes:
            try:
                link = b.find_all('text')[0].find_all('div', attrs={'class': 'title'})[0].a['href']
                title = b.find_all('text')[0].find_all('div', attrs={'class': 'title'})[0]
                data = b.find_all('text')[0].find_all('div', attrs={'class': 'data'})[0].text
                info = b.find_all('text')[0].find_all('div', attrs={'class': 'info'})[0]

                id = re.search('adv=(.*)$', link).group(1)
                place, labels = get_place_and_labels(clean_text(title.a.text.replace('град София,', '')))


                area = re.search('(^[^А-Яа-я\.]*)', data.split(',')[1].replace(' ', '')).group(1) if len(data.split(',')) > 1 and re.search('(^[^А-Яа-я\.]*)', data.split(',')[1].replace(' ', '')) else '0'
                price = clean_text(title.find_all('span')[0].text)
                price_orig = price

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

                typ = clean_text(data.split(',')[0])
                agency = clean_text(info.a['href']) if len(info.find_all('a')) > 0 else ''

                offers = offers.append({'link': sale_url + link,
                                        'id': id,
                                        'type': typ,
                                        'place': place,
                                        'price': price,
                                        'area': area,
                                        'labels': labels,
                                        'description': clean_text(info.text),
                                        'currency': currency,
                                        'agency': agency}, ignore_index=True)

            except Exception as e:
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