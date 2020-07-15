import bs4
import os
import requests
import pandas as pd
import re
import time
from tqdm import tqdm
from datetime import datetime
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options


base_url = 'https://etuovi.com'
search_url = 'https://www.etuovi.com/myytavat-asunnot/{}'
cities = ['espoo', 'helsinki', 'vantaa']
offers_file = "etuovi_"

def get_page_count(page):
    max_page = max([int(re.search('([\d]+)', a.text).group(1)) for a in page.findAll('button', attrs={'class':'Pagination__button__3H2wX'}) if re.search('[\d]+', a.text) is not None])
    return int(max_page)


def gather_new_articles():
    offers = crawlLinks() 
    offers['is_for_sale'] = True

    return offers


def extract_details(b, city):
    id = b.findAll('a', attrs={'class': 'styles__cardLink__2Oh5I'})[0]['id']
    link = b.findAll('a', attrs={'class': 'styles__cardLink__2Oh5I'})[0]['href']
    meta = b.find('div', attrs={'class': 'styles__infoArea__2yhEL'})
    typ = meta.find('div', attrs={'class': 'styles__cardTitle__14F5m'}).find('h5').text
    details = typ.split('|')[1] if '|' in typ else ''
    typ = typ.split('|')[0]
    address = meta.find('div', attrs={'class': 'styles__cardTitle__14F5m'}).find('h4').text
    meta_numbers = b.find('div', attrs={'class': 'styles__itemInfo__oDGHu'}).div.findAll('div')
    price = meta_numbers[0].text.replace('Hinta', '').replace('\xa0', '')
    price = re.search('([\d]+)(?:[\d,]+)?€$', price).group(1) if  re.search('([\d]+)(?:[\d,]+)?€$', price) is not None else '0'
    area = meta_numbers[1].text.replace('Koko', '').replace(' m²', '').replace(',', '.')
    year = meta_numbers[2].text.replace('Vuosi', '')

    labels = ', '.join([l.text.replace('\xa0', '') for l in b.findAll('div', attrs={'class':'theme__chip__3gBsZ'})])

    return {'link': base_url + link,
            'id': id,
            'type': typ,
            'details': details,
            'labels': labels,
            'city': city,
            'place': address,
            'price': price,
            'area': area,
            'year': year}


def crawlLinks():
    offers = pd.DataFrame()
    options = Options()
    options.headless = True
    browser = webdriver.Chrome(ChromeDriverManager().install(), options=options)

    for city in cities:
        browser.get(search_url.format(city))
        time.sleep(10)
        page = bs4.BeautifulSoup(browser.page_source, 'html')
        current_page = 0
        max_page = get_page_count(page)
        pbar = tqdm(total=max_page)
        
        while current_page < max_page-1:
            current_page = int(page.find('button', attrs={'class': 'Pagination__selected__1MsKZ'}).text)
            pbar.update(1)
            boxes = page.findAll('div', attrs={'class': 'ListPage__cardContainer__39dKQ'})
            for b in boxes:
                try:
                    offers = offers.append(extract_details(b, city), ignore_index=True)
                    
                except Exception as e:
                    print(address)
                    continue

            element = browser.find_element_by_id('paginationNext')
            browser.execute_script("arguments[0].click();", element)
            time.sleep(5)
            page = bs4.BeautifulSoup(browser.page_source, 'html')

        offers = offers.append(extract_details(b, city), ignore_index=True)
        pbar.close()

    browser.close()

    return offers


if __name__ == '__main__':
	gather_new_articles()