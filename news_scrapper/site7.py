#!/usr/bin/env python
# -*- coding: utf-8 -*-
import bs4
import requests
import pandas as pd
from urllib.parse import urlsplit
from urllib.parse import unquote
import re
from helpers import clean_text, replace_month_with_digit


def gather_new_articles(site):
    request = requests.get(site)
    soup = bs4.BeautifulSoup(request.text, 'lxml')

    all_articles = set([site + unquote(a['href']) for a in soup.find_all('a', href=True, title=True) if
                       '/topic/' not in a['href'] and re.match('/\w+/', a['href'])])

    all_articles = crawlLinks(all_articles)

    return all_articles


def crawlLinks(links):
    articles_content = pd.DataFrame()

    for link in links:
        try:
            rq = requests.get(link)
            domain = "{0.netloc}".format(urlsplit(link))
            category = re.search(domain + '/([^/]+)', link).group(1)

            if rq.status_code == 200:
                page = bs4.BeautifulSoup(rq.text, 'lxml')
                info = page.select('#news_details')[0]

                headline = info.h1.text.strip()
                subtitle = info.h2.text.strip()

                meta = info.select('.info')[0].select('div')

                # 30 Декември, 2019 15:26
                articleDate = meta[0].text.split('Публикувана:')[1].strip()
                month_name = re.search('([а-яА-Я]+)', articleDate)
                month_name = month_name.group(1) if month_name is not None else None
                articleDate = articleDate.replace(month_name, replace_month_with_digit(
                    month_name)) if month_name is not None else articleDate
                articleDate = pd.to_datetime(articleDate, format='%d %m, %Y %H:%M')

                meta = meta[1].text.strip()
                comments = re.search('(^\d+)', meta).group(1)
                views = re.search('(\d+)$', meta).group(1)
                author = page.select('.linksProfile')[0].text

                article_body = page.select('#news_content')[0].select('p')
                article_text = ' '.join([clean_text(par.text)
                                         for par in article_body if '<' not in par.text])
                tags = ' - '.join(
                    [clean_text(tag.text) for tag in page.select('.tags')[0].select('a') if tag != ',' and tag != "\n"]) \
                    if len(page.select('.tags')) > 0 else None

                #shares
                # shares = page.select('.inlineBlock')[1].select('.span')[-1].text
                
                articles_content = articles_content.append({'link': link,
                                                            'title': clean_text(headline),
                                                            'subtitle': clean_text(subtitle),
                                                            'author': clean_text(author),
                                                            'date': articleDate,
                                                            'category': category,
                                                            'comments': clean_text(comments),
                                                            # 'shares': shares,
                                                            'views': clean_text(views),
                                                            'tags': tags,
                                                            'article_text': article_text},
                                                           ignore_index=True)
        except:
            continue

    return articles_content
