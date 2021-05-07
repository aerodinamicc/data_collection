import bs4
import os
import requests
import pandas as pd
import re
import time
from tqdm import tqdm
from datetime import datetime
from helpers import clean_text
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options


base_url = 'https://address.bg/'
search_url = "https://address.bg/{}/sofia/l4451?page={}"
offers_file = "address_"

def get_page_count(page):
    max_page = max([int(re.search('([\d]+)', a.text).group(1)) for a in page.findAll('a', attrs={'class':'page-link'}) if re.search('([\d]+)', a.text) is not None])
    return max_page


def gather_new_articles():
    options = Options()
    options.headless = True
    options.add_argument('log-level=3')
    #options.add_argument('--no-sandbox')
    browser = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
    browser.get(search_url.format('sale', '1'))    
    time.sleep(5)
    page_sale = bs4.BeautifulSoup(browser.page_source, features='html.parser')
    page_count_sale = get_page_count(page_sale)

    browser.get(search_url.format('rent', '1'))
    time.sleep(5)
    page_rent = bs4.BeautifulSoup(browser.page_source, features='html.parser')   
    page_count_rent = get_page_count(page_rent)

    offers_sale = crawlLinks('sale', page_count_sale)
    offers_rent = crawlLinks('rent', page_count_rent)       
    offers_sale['is_for_sale'] = True
    offers_rent['is_for_sale']= False
    offers = pd.concat([offers_rent, offers_sale], ignore_index=True)
    browser.close()

    offers.to_csv('address.bg_3004.csv', index=False, sep='\t')

    return offers


def crawlLinks(type_of_offering, page_count):
    offers = pd.DataFrame()
    options = Options()
    options.headless = True
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36")
    options.add_argument('log-level=3')
    #options.add_argument('--no-sandbox')
    browser = webdriver.Chrome(ChromeDriverManager().install(), options=options)

    for page_n in tqdm(range(1, page_count + 1)):
        browser.get(search_url.format(type_of_offering, str(page_n)))
        time.sleep(3)
        page = bs4.BeautifulSoup(browser.page_source, features='html.parser', from_encoding="windows-1252")
        boxes = page.findAll('div', attrs={'class': 'offer-card'})
        #import pdb; pdb.set_trace()
        for b in boxes:
            try:
                b = b.select('.content')[0]
                link = b.a['href']
                id = re.search('([\d]+$)', link).group(1)
                meta = b.findAll('div', attrs={'class': 'row'})
                nbhd =  meta[0].findAll('small', attrs={'class': 'gray-m'})[0].text.replace('София, ', '') \
                            if len(meta[0].findAll('small', attrs={'class': 'gray-m'})) > 0 else ''
                area = meta[0].findAll('small', attrs={'class': 'gray-d'})[0].text.replace(' кв.м.', '') \
                            if len(meta[0].findAll('small', attrs={'class': 'gray-m'})) > 0 else '0'
                price = b.findAll('small', attrs={'class': 'price'})[0].text.replace('\t', '').replace('\n', '').replace(' ', '') \
                            if len(b.findAll('small', attrs={'class': 'price'})) else '0'
                typ = meta[1].findAll('small')[1].text \
                            if len(meta) > 0 and len(meta[1].findAll('small')) > 1 else ''
                labels = b.findAll('div', attrs={'class': 'building'})[0].text if len(b.findAll('div', attrs={'class': 'building'})) > 0 else ''

                data = {'link': link,
                        'id': id,
                        'type': typ,
                        'labels': labels,
                        'place': nbhd,
                        'price': price,
                        'area': area}

                offers = offers.append(data, ignore_index=True)

                #print(data)

            except Exception as e:
                print(e)
                continue
        
    browser.close()

    return offers


if __name__ == '__main__':
	gather_new_articles()