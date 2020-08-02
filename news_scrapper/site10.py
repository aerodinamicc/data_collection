#!/usr/bin/env python
# -*- coding: utf-8 -*-
import bs4
import requests
import pandas as pd
from urllib.parse import urlsplit
from urllib.parse import unquote
import re
from tqdm import tqdm
from helpers import clean_text, replace_month_with_digit

def gather_new_articles(site):
    request = requests.get(site)
    soup = bs4.BeautifulSoup(request.text, features="html.parser")

    all_articles = list(set(soup.findAll('a', attrs={'href':re.compile('^/.*\.html$')})))
    #import pdb; pdb.set_trace()
    articles_content = pd.DataFrame()
    for a in all_articles:
        try:
            title = a['title']
            link = site + a['href']
            comments = a.find('span', attrs={'class':'cmc'}).text if a.find('span', attrs={'class':'cmc'}) else ''
            views = a.find('span', attrs={'class':'cmv'}).text if a.find('span', attrs={'class':'cmv'}) else ''
            date = a.find('span', attrs={'class':'cmd'}).text if a.find('span', attrs={'class':'cmd'}) else ''
            desc = a.find('span', attrs={'class':'short-desc'}).text if a.find('span', attrs={'class':'short-desc'}) else ''

            articles_content = articles_content.append({'link': link,
                                                        'title': clean_text(title),
                                                        'comments': clean_text(comments),
                                                        'views': clean_text(views),
                                                        'category': re.search('frognews\.bg//(\w+)', link).group(1) if re.search('frognews\.bg//(\w+)', link) else '',
                                                        'date': clean_text(date),
                                                        'subtitle': clean_text(desc)},
                                                        ignore_index=True)
        except:
            continue



    return articles_content


