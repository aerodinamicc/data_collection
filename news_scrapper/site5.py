#!/usr/bin/env python
# -*- coding: utf-8 -*-
import bs4
import requests
import pandas as pd
from urllib.parse import urlsplit
import re
from helpers import clean_text, replace_month_with_digit


def gather_new_articles(site):
    request = requests.get(site)
    soup = bs4.BeautifulSoup(request.text, 'lxml')

    all_articles = set([site + a['href'] for a in soup.find_all('a', href=True) if
                  '/article/' in a['href'] and a['href'].startswith('/')])

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
                meta = page.select('.head')[0]
                headline = meta.h1.text.strip()

                # Вижте 50-те най-четени мнения в сайта ни за годината
                if headline == '':
                    continue
                info = clean_text(meta.select('.article-date')[0].text.split('(')[0])

                # 30.12.2019 10:33
                articleDate = info.split(';')[0]
                month_name = re.search('([а-яА-Я]+)', articleDate)
                month_name = month_name.group(1) if month_name is not None else None
                articleDate = articleDate.replace(month_name, replace_month_with_digit(
                    month_name)) if month_name is not None else articleDate
                articleDate = pd.to_datetime(articleDate, format='%d.%m.%Y  %H:%M')

                author = info.split(';')[1] if ';' in info else None
                views = requests.get('https://www.24chasa.bg/Article/{id}/4'.format(id=re.search('(\d+)$', link).group(1))).text
                article_text = ' '.join([clean_text(par.text) for par in page.select('.content')[0].select('p')]).split('Tweet')[0]

                # shares - will need selenium for that
                # shares = page.select('.inlineBlock')[1].select('.span')[-1].text
                
                articles_content = articles_content.append({'link': link,
                                                            'title': clean_text(headline),
                                                            'author': clean_text(author),
                                                            'date': articleDate,
                                                            #'shares': shares,
                                                            'views': clean_text(views),
                                                            'category': category,
                                                            'article_text': article_text},
                                                           ignore_index=True)
        except:
            continue

    return articles_content
