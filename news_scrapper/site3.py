#!/usr/bin/env python
# -*- coding: utf-8 -*-
import bs4
import requests
from datetime import datetime
import pandas as pd
from urllib.parse import urlsplit
import re
from tqdm import tqdm
from helpers import clean_text, replace_month_with_digit


def gather_new_articles(site):
    request = requests.get(site)
    soup = bs4.BeautifulSoup(request.text, features="html.parser")

    all_articles = set([a['href'] for a in soup.find_all('a', href=True) if a['href'].startswith(site) and a['href'].endswith('.html')])
    articles = crawlLinks(all_articles)

    return articles


def crawlLinks(links):
    articlesContent = pd.DataFrame()

    for link in tqdm(list(links)):
        try:    
            rq = requests.get(link)
            domain = "{0.netloc}".format(urlsplit(link))
            category = re.search(domain + '/([^/]+)', link).group(1)
            if rq.status_code == 200:
                page = bs4.BeautifulSoup(rq.text, features="html.parser")

                if page.find({'class': 'article-post'}):
                    body = page.select('.article-post')[0]
                    headline = body.select('h1')[0].text if len(body.select('h1')) else ''
                    subtitle = None

                    #metadata
                    location = body.select('.location')[0].text if len(body.select('.location')) else ''
                    articleDate = body.select('.fa-calendar')[0].text if len(body.select('.fa-calendar')) else ''
                    views = body.select('.fa-eye')[0].text if len(body.select('.fa-eye')) else ''
                    comments = body.select('.fa-comments-o')[0].text if len(body.select('.fa-comments-o')) else ''
                    comments = comments.split(" ")[0] if comments != '' else ''
                    tags = ' - '.join([tag['a'].text for tag in body.select('.tags').select('li')])
                else: 
                    headline = page.select('.post-title')[0].text if len(page.select('.post-title')) else ''
                    subtitle = page.select('.post-subtitle')[0].text if len(page.select('.post-subtitle')) else ''

                    #metadata
                    simpleShare = page.select('.simple-share')[0] if len(page.select('.simple-share')) > 0 else ''
                    li = simpleShare.find_all('li')
                    location = li[0].text if len(li) > 0 else ''
                    articleDate = li[1].text if len(li) > 1 else ''
                    views = li[2].text if len(li) > 2 else ''
                    views = views.split(" ")[0] if views != '' else ''
                    comments = li[3].text if len(li) > 3 else ''
                    comments = comments.split(" ")[0] if comments != '' else ''
                    tags = ' - '.join([tag.a.text for tag in page.select('.tags-widget')[0].select('li')[1:]]) if len(page.select('.tags-widget')) > 0 else ''

                # 30 Дек. 2019, 16:13
                if articleDate != '':
                    month_name = re.search('([а-яА-Я]+)', articleDate)
                    if month_name is not None:
                        month_name = month_name.group(1)
                        articleDate = articleDate.replace(month_name, replace_month_with_digit(month_name))
                        articleDate = pd.to_datetime(articleDate, format='%d %m %Y,  %H:%M')

                article_text = clean_text(page.select('.post-content')[0].select('div')[2].text) if len(page.select('.post-content')) > 0 else ''

                articlesContent = articlesContent.append({'link': link,
                                                          'title': clean_text(headline),
                                                          'subtitle': clean_text(subtitle),
                                                          'location': clean_text(location),
                                                          'comments': clean_text(comments),
                                                          'date': articleDate,
                                                          'views': clean_text(views),
                                                          'category': category,
                                                          'tags': clean_text(tags),
                                                          'article_text': article_text},
                                                         ignore_index=True)
        except:
            continue

    return articlesContent