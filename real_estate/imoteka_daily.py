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


base_url = 'https://imoteka.bg'
search_url = "https://imoteka.bg/{}/sofiya?locations=4451&page={}"
offers_file = "imoteka_"

def get_page_count(page):
    max_page = max([int(re.search('([\d]+)', a.text).group(1)) for a in page.findAll('a', attrs={'class':'page-link'}) if re.search('([\d]+)', a.text) is not None])
    return max_page


def gather_new_articles():
    options = Options()
    options.headless = True
    options.add_argument('log-level=3')
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
    offers = pd.concat([offers_rent, offers_sale], ignore_index=True)       
    offers['is_for_sale'] = offers['is_for_sale'].astype(bool)
    browser.close()

    return offers


def crawlLinks(type_of_offering, page_count):
    offers = pd.DataFrame()
    options = Options()
    options.headless = True
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36")
    options.add_argument('log-level=3')
    browser = webdriver.Chrome(ChromeDriverManager().install(), options=options)

    for page_n in tqdm(range(1, page_count + 1)):
        browser.get(search_url.format(type_of_offering, str(page_n)))
        time.sleep(3)
        page = bs4.BeautifulSoup(browser.page_source, features='html.parser')
        boxes = page.findAll('div', attrs={'class': 'list__item'})

        for b in boxes:
            try:
                id = re.search('([\d]+)', b.select('.list__img-id')[0].text).group(1)
                meta = b.findAll('div', attrs={'class': 'list__info-container'})[0].findAll('div', attrs={'class': 'list__info'})[0].findAll('div')
                nbhd =  meta[1].text
                area = meta[2].text.replace('Квадратура: ', '').replace('M2', '').strip()
                area = area if len(area) > 0 else '0'
                price = meta[3].text.replace('Цена: ', '')
                if 'EUR' in price:
                    price = price.replace('EUR', '').replace(' ', '')
                    currency = 'EUR'
                elif 'BGN' in price:
                    price = str(round(float(price.replace('BGN', '').replace(' ', '')) / 1.9558))
                    currency = 'EUR'

                typ = b.findAll('span', attrs={'class':re.compile('truncate-label')})[0].text
                is_for_sale = typ.split('/')[0].strip() == 'Продажба'
                typ = '/'.join(typ.split('/')[1:])
                link = b.select('.list__info-container')[0].findAll('a', text=re.compile('Вижте в детайли'))[0]['href']
                labels = ', '.join([l['data-tooltip'] for l in b.findAll('div', attrs={'data-tooltip': re.compile('.*')}) if l['data-tooltip'].strip() not in ['Преглеждания на сайта', 'Спрямо всички оферти в района','Добави в любими']])
                views = ''.join([d.text for d in b.findAll('g')[0].findAll('text')])
                over_under = b.findAll('div', attrs={'data-tooltip': re.compile('Спрямо всички оферти в района')})[0].findAll('div', attrs={'class':re.compile('list__price-text')})[0].text 

                offers = offers.append({'link': base_url + link,
                                        'id': id,
                                        'is_for_sale': is_for_sale,
                                        'currency': currency,
                                        'type': typ,
                                        'labels': labels,
                                        'views': views,
                                        'over_under': over_under,
                                        'place': nbhd,
                                        'price': price,
                                        'area': area}, ignore_index=True)

            except Exception as e:
                print(e)
                continue
        
    browser.close()

    return offers


if __name__ == '__main__':
	gather_new_articles()