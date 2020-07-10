import bs4
import os
import requests
import pandas as pd
import re
from tqdm import tqdm
from datetime import datetime


base_url = 'https://arcoreal.bg'
search_url = 'https://www.arcoreal.bg/%D0%BE%D1%84%D0%B5%D1%80%D1%82%D0%B8?t={}&ca=3&rt%5B%5D=1&rt%5B%5D=2&rt%5B%5D=3&rt%5B%5D=4&rt%5B%5D=multi&rt%5B%5D=t-11&rt%5B%5D=t-14&rt%5B%5D=t-1&rt%5B%5D=t-3&rt%5B%5D=t-4&pfr=&pto=&afr=&ato=&ffr=-100&fto=100&country-name=1&c=1&l%5B%5D=&page={}'
_is_sale = 2
_is_rent = 4
offers_file = "arco_"

def get_page_count(page):
    max_page = max([int(re.search('([\d]+)', a.text).group(1)) for a in page.findAll('a', attrs={'class':'pgg-link'}) if re.search('[\d]+', a.text) is not None])
    return max_page


def gather_new_articles(current_date):
    #resp_sale = requests.get(search_url.format('sale', '1'))
    #page_sale = bs4.BeautifulSoup(resp_sale.text, 'html')
    #resp_rent = requests.get(search_url.format('rent', '1'))
    #page_rent = bs4.BeautifulSoup(resp_rent.text, 'html')

    with open('C:/Users/shadow/Downloads/arco_sale.html', 'r', encoding='utf-8') as f:
        resp_sale = f.read()

    with open('C:/Users/shadow/Downloads/arco_rent.html', 'r', encoding='utf-8') as f:
        resp_rent = f.read()

    page_sale = bs4.BeautifulSoup(resp_sale, 'html')
    page_rent = bs4.BeautifulSoup(resp_rent, 'html')

    page_count_sale = get_page_count(page_sale)
    page_count_rent = get_page_count(page_rent)

    offers_sale = crawlLinks(_is_sale, 1) #page_count_sale)  
    offers_rent = crawlLinks(_is_rent, 1) #page_count_rent)    
    offers = pd.concat([offers_rent, offers_sale], ignore_index=True)       

    offers = offers[['id', 'type', 'is_for_sale', 'place', 'price' ,'area', 'link', 'description']]
    offers['is_for_sale'] = offers['is_for_sale'].astype(bool)										   
    if not os.path.exists('output'):
        os.mkdir('output')
    offers.to_csv('output/' + offers_file + current_date + '.tsv', sep='\t', index=False)

    return offers


def crawlLinks(type_of_offering, page_count):
    offers = pd.DataFrame()

    for page_n in tqdm(range(1, page_count + 1)):
        #resp = requests.get(search_url.format(type_of_offering, str(page_n)))
        #page = bs4.BeautifulSoup(resp.text, 'html')
        with open('C:/Users/shadow/Downloads/arco_sale.html' if type_of_offering == _is_sale \
                else 'C:/Users/shadow/Downloads/arco_rent.html', 'r', encoding='utf-8') as f:
            resp = f.read()

        page = bs4.BeautifulSoup(resp, 'html')

        boxes = page.findAll('div', attrs={'class': 'offer-box'})

        import pdb; pdb.set_trace()
        for b in boxes:
            try:
                id = b.select('.id')[0].text.replace('ID: ', '')
                link = b.findAll('div', attrs={'class':'img'})[0].findAll('a')[0]['href']
                price = b.select('.price')[0].text.strip()
                place = b.select('.location')[0].text.replace('София (град),', '').strip()
                details = b.select('.details')[0].text.split(',')
                is_for_sale = details[0] == 'Продава'
                typ = details[1]
                area = details[2].replace(' кв.м.', '')
                desc = b.select('.text')[0].text.strip()

                offers = offers.append({'id': id,
                                        'is_for_sale': is_for_sale,
                                        'type': typ,
                                        'place': place,
                                        'price': price,
                                        'area': area,
                                        'link': link,
                                        'description': desc}, ignore_index=True)

            except Exception as e:
                print(e)
                continue

    return offers


if __name__ == '__main__':
	current_date = str(datetime.now().date())
	gather_new_articles(current_date)