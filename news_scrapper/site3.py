#!/usr/bin/env python
# -*- coding: utf-8 -*-
import bs4
import requests
from datetime import datetime
import pandas as pd
from urllib.parse import urlsplit
import re
from helpers import clean_text, replace_month_with_digit


def gather_new_articles(site):
    request = requests.get(site)
    soup = bs4.BeautifulSoup(request.text, 'lxml')

    most_read = set([art.a['href'] for art in soup.find({id: 'mostRead'}).select('.simple-post') if art.a['href'].endswith('.html')])
    all_articles = set([a['href'] for a in soup.find_all('a', href=True) if a['href'].startswith(site) and a['href'].endswith('.html')])
    all_articles = all_articles.difference(most_read)

    most_read = crawlLinks(most_read)
    most_read['section'] = 'most_read'
    all_articles = crawlLinks(all_articles)
    all_articles['section'] = None

    articles = pd.concat([most_read, all_articles])

    return articles


def crawlLinks(links):
    articlesContent = pd.DataFrame()

    for link in links:
        try:
            rq = requests.get(link)
            domain = "{0.netloc}".format(urlsplit(link))
            category = re.search(domain + '/([^/]+)', link).group(1)

            if rq.status_code == 200:
                page = bs4.BeautifulSoup(rq.text, 'lxml')

                if page.find({'class': 'article-post'}):
                    body = page.select('.article-post')[0]
                    headline = body.select('h1')[0].text
                    subtitle = None

                    #metadata
                    location = body.select('.location')[0].text
                    articleDate = body.select('.fa-calendar')[0].text
                    views = body.select('.fa-eye')[0].text
                    comments = body.select('.fa-comments-o')[0].text
                    comments = comments.split(" ")[0]
                    tags = [tag['a'].text for tag in body.select('.tags').select('li')]
                else: 
                    headline = page.select('.post-title')[0].text
                    subtitle = page.select('.post-subtitle')[0].text

                    #metadata
                    simpleShare = page.select('.simple-share')[0]
                    li = simpleShare.find_all('li')
                    location = li[0].text
                    articleDate = li[1].text
                    views = li[2].text
                    views = views.split(" ")[0]
                    comments = li[3].text
                    comments = comments.split(" ")[0]
                    tags = ' - '.join([tag.a.text for tag in page.select('.tags-widget')[0].select('li')[1:]])

                # 30 Дек. 2019, 16:13
                month_name = re.search('([а-яА-Я]+)', articleDate)
                month_name = month_name.group(1) if month_name is not None else None
                articleDate = articleDate.replace(month_name, replace_month_with_digit(month_name)) \
                    if month_name is not None else articleDate
                articleDate = pd.to_datetime(articleDate, format='%d %m. %Y,  %H:%M')

                article_text = clean_text(page.select('.post-content')[0].select('div')[2].text)

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