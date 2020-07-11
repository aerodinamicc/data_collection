import bs4
import os
import requests
import pandas as pd
import re
from tqdm import tqdm
from datetime import datetime
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager


base_url = 'https://etuovi.com'
search_url = 'https://www.etuovi.com/myytavat-asunnot/{}'
cities = ['espoo', 'helsinki', 'vantaa']
offers_file = "etuovi_"

def get_page_count(page):
    max_page = max([int(re.search('([\d]+)', a.text).group(1)) for a in page.findAll('button', attrs={'class':'Pagination__button__3H2wX'}) if re.search('[\d]+', a.text) is not None])
    return max_page


def gather_new_articles(current_date):
    offers = crawlLinks() 
    offers = offers[['link', 'id', 'type', 'city', 'address', 'price', 'area', 'labels', 'year']]#, 'seller']]									   
    if not os.path.exists('output'):
        os.mkdir('output')
    offers.to_csv('output/' + offers_file + current_date + '.tsv', sep='\t', index=False)

    return offers


def crawlLinks():
    offers = pd.DataFrame()
    browser = webdriver.Chrome(ChromeDriverManager().install())

    for city in cities:
        browser.get(search_url.format(city))
        time.sleep(7)
        page = bs4.BeautifulSoup(browser.page_source, 'html')
        current_page = int(b.find('button', attrs={'class': 'Pagination__selected__1MsKZ'}).text)
        next_page = b.find('button', attrs={'id': 'paginationNext'})
        max_page = get_page_count(page)
        
        while current_page <= max_page:
            current_page = int(b.find('button', attrs={'class': 'Pagination__selected__1MsKZ'}).text)
            boxes = page.findAll('div', attrs={'class': 'ListPage__cardContainer__39dKQ'})
            for b in boxes:
                try:
                    id = b.findAll('a', attrs={'class': 'styles__cardLink__2Oh5I'})[0]['id']
                    link = b.findAll('a', attrs={'class': 'styles__cardLink__2Oh5I'})[0]['href']
                    meta = b.find('div', attrs={'class': 'styles__infoArea__2yhEL'})
                    typ = meta.find('div', attrs={'class': 'styles__cardTitle__14F5m'}).find('h5').text
                    address = meta.find('div', attrs={'class': 'styles__cardTitle__14F5m'}).find('h4').text
                    meta_numbers = b.find('div', attrs={'class': 'styles__itemInfo__oDGHu'}).div.findAll('div')
                    price = meta_numbers[0].text.replace('Hinta', '').replace('\xa0', '')
                    area = meta_numbers[1].text.replace('Koko', '').replace(' m²', '')
                    year = meta_numbers[2].text.replace('Vuosi', '')

                    labels = ', '.join([l.text.replace('\xa0', '') for l in b.findAll('div', attrs={'class':'theme__chip__3gBsZ'})])
                    '''seller = b.find('div', attrs={'class': 'ItemOfficeLogo__itemOfficeLogo__2v-Vr'}).find('img')['alt'] \
                                    if len(b.findAll('div', attrs={'class': 'ItemOfficeLogo__itemOfficeLogo__2v-Vr'})) > 0 else \
                                    'private seller' if len(b.findAll('span', attrs={'class': 'ItemPrivateSellerLogo__wrapper__3qv_o'})) > 0 else ''
                    '''

                    offers = offers.append({'link': link,
                                            'id': id,
                                            'type': typ,
                                            'labels': labels,
                                            #'city': city,
                                            'address': address,
                                            'price': price,
                                            'area': area,
                                            #'seller': seller,
                                            'year': year}, ignore_index=True)

                except Exception as e:
                    print(address)
                    continue

            browser.find_element_by_id("paginationNext").click()
            time.sleep(5)
            page = bs4.BeautifulSoup(browser.page_source, 'html')
        
        browser.close()

    return offers


if __name__ == '__main__':
	current_date = str(datetime.now().date())
	gather_new_articles(current_date)