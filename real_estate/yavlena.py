import argparse
import bs4
import requests
import pandas as pd
import re
from tqdm import tqdm
from helpers import clean_text, replace_month_with_digit, months
import time


base_url = 'https://yavlena.bg'
base_search_url = "drundni_{}_maini"
offers_file = "yavlena.bg_"


def get_neighbourhood_links():
    rq = request.get(base_search_url)
    page = bs4.BeautifulSoup(rq, 'html')
    neighbourhoods = page.findAll('input',
                                  attrs={'name': 'quarters',
                                        'type':'checkbox'})
    #tuple(neighbourhood_name, link)
    neighbourhoods = [(n['data-quarters'], base_search_url.format(n['value'])) for n in neighbourhoods]
    
    return neighbourhoods


def gather_new_articles(current_date):
    neighbourhoods = get_neighbourhood_links()

    offers = crawlLinks(neighbourhoods)            
    offers = offers[['link', 'type', 'extras', 'place', 'lon', 'lat', 'price', 'area', 'description']]											   
    offers.to_csv(offers_file + current_date + '.tsv', sep='\t', index=False)

    return offers


def crawlLinks(neighbourhoods):
    offers = pd.DataFrame()
    browser = webdriver.Chrome(executable_path='./webdriver/chromedriver.exe')
	
    for nbhd, nbhd_link in tqdm(neighbourhoods):
        browser.get(nbhd_link)
        time.sleep(10)
        page = bs4.BeautifulSoup(browser.page_source, 'html') #, from_encoding="utf-8")
        load_more = True
        
        while load_more:
            # click Зареди Още
            time.sleep(2)
            page = bs4.BeautifulSoup(browser.page_source, 'html')
            load_more = len(page.findAll('a', text = 'Зареди още', attrs={'class': 'load-more-results-list'})) > 0

        # scrape all offer boxes for that neighbourhood
        boxes = page.findAll('article', attrs={'class': 'card-list-item'})

        for b in boxes:
            price = b.select('.price-label')[0].text.replace('&nbsp;', '') if len(b.select('.price-label')) > 0 else 0
            desc = b.select('.full-text')[0].text if len(b.select('.full-text')) > 0 else 0
            tbody = b.findAll('tbody')[0]
            type_ = tbody.findAll('tr')[0].findAll('td')[1].h3.text
            link = tbody.findAll('tr')[0].findAll('td')[1].h3.a['href']
            lat = tbody.findAll('tr')[1].findAll('td')[0].a['data-map-lat']
            lng = tbody.findAll('tr')[1].findAll('td')[0].a['data-map-lng']
            area = tbody.findAll('tr')[2].findAll('td')[0].a.text

            extras = {}
            for ex in b.select('.extras')[0].findall('div'):
                spans = ex.findall('span')

                if len(spans) == 1:
                    extras[spans[0]['title']] = 0
                elif len(spans) == 2:
                    extras[spans[1]['title']] = spans[0].text

            current_box = pd.DataFrame(data={'link': link,
                                               'type': type_,
                                               'extras': extras,
                                               'place': nbhd,
                                               'lon': lng,
                                               'lat': lat,
                                               'price': price,
                                               'area': area,
                                               'description': desc)

            offers = pd.concat([offers, current_box], ignore_index=True)

        except Exception as e:
            print(e)
            continue

    browser.close()

    return offers


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-current_date', required=True, help="MMDD")
	parsed = parser.parse_args()
	current_date = parsed.current_date
	gather_new_articles(current_date)