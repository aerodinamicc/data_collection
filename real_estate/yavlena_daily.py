import argparse
import bs4
import os
import requests
import pandas as pd
import re
from tqdm import tqdm
from helpers import clean_text, replace_month_with_digit, months
import time
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from datetime import datetime



base_url = 'https://yavlena.com'
base_search_url = "https://www.yavlena.com/properties/all/sofia/sofia/?{}ptype=Room,Studio,OneBedroomApartment,TwoBedroomApartment,ThreeBedroomApartment,FourBedroomApartment,FivePlusBedroomApartment,House,HouseWholeFloor&view=List"
offers_file = "yavlena_"


def get_neighbourhood_links():
    options = Options()
    options.headless = True
    browser = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    browser.get(base_search_url.format(''))
    page = bs4.BeautifulSoup(browser.page_source, 'html')
    neighbourhoods  = page.findAll('input', attrs={'name': 'quarters', 'type':'checkbox'})
    browser.close()
    #tuple(neighbourhood_name, link)
    neighbourhoods = [(n['data-quarter'], base_search_url.format("quarter=" + n['value'] + '&')) for n in neighbourhoods]
    
    return neighbourhoods


def gather_new_articles():
    neighbourhoods = get_neighbourhood_links()
    offers = crawlLinks(neighbourhoods)   
    offers['is_for_sale'] = offers['link'].apply(lambda x: 'link' in x)     

    return offers


def crawlLinks(neighbourhoods):
    offers = pd.DataFrame()
    options = Options()
    options.headless = True
    browser = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)

    for nbhd, nbhd_link in tqdm(neighbourhoods):
        browser.get(nbhd_link)
        time.sleep(7)
        page = bs4.BeautifulSoup(browser.page_source, 'html')
        load_more = len(page.findAll('div', attrs={'class': 'load-more-holder', 'style':re.compile('block')})) > 0
        
        #when there more results to load
        while load_more:
            browser.find_element_by_class_name("load-more-results-list").click()
            time.sleep(2)
            page = bs4.BeautifulSoup(browser.page_source, 'html')
            load_more = len(page.findAll('div', attrs={'class': 'load-more-holder', 'style':re.compile('block')})) > 0

        # scrape all offer boxes for that neighbourhood
        boxes = page.findAll('article', attrs={'class': 'card-list-item'})

        for b in boxes:
            try:
                price = b.select('.price-label')[0].text.replace('&nbsp;', '').replace('\xa0', '').replace('€', '').strip() if len(b.select('.price-label')) > 0 and len(b.select('.price-label')[0].text) > 0 else '0'
                desc = b.select('.full-text')[0].text.replace('\n', '').replace('\t', '') if len(b.select('.full-text')) > 0 else 0
                tbody = b.findAll('tbody')[0]
                is_selling = tbody.findAll('tr')[0].findAll('td')[0].h3.text
                type_ = tbody.findAll('tr')[0].findAll('td')[1].h3.text
                link = tbody.findAll('tr')[0].findAll('td')[1].h3.a['href']
                id = re.search('\/([^\/]+)(?:\/rent)?$', link).group(1) if re.search('\/([^\/]+)(?:\/rent)?$', link) is not None else '0'
                lat = tbody.findAll('tr')[1].findAll('td')[0].a['data-map-lat']
                lng = tbody.findAll('tr')[1].findAll('td')[0].a['data-map-lng']
                area = tbody.findAll('tr')[2].findAll('td')[0].text.replace('кв.м.', '').strip()
                is_for_sale = 'rent' in link

                extras = {}
                exs = b.select('.extras')[0]
                exs = exs.select('div')
                for ex in exs:
                    spans = ex.select('span')

                    if len(spans) == 1:
                        extras[spans[0]['title']] = 0
                    elif len(spans) == 2:
                        extras[spans[1]['title']] = spans[0].text

                offers = offers.append({'link': base_url + link,
                                        'id': id,
                                        'type': type_,
                                        'details': str(extras),
                                        'place': nbhd,
                                        'lon': lng,
                                        'lat': lat,
                                        'price': price,
                                        'area': area,
                                        'description': desc}, ignore_index=True)

            except Exception as e:
                print(e)
                continue
    
    browser.close()

    return offers


if __name__ == '__main__':
	gather_new_articles()