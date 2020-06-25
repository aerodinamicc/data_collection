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

    all_articles = set([a['href'] for a in soup.find_all('a', href=True) if
                  re.match('^https://\w+\.dir\.bg/\w+/', a['href'])
                  and '/topic/' not in a['href']\
                  and '/comments/' not in a['href']])
#
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
                titles = page.select('.text-wrapper')[0]
                headline = titles.h2.text
                subtitle = page.select('.text-wrapper')[0].p.text
                meta = page.select('.additional-info')[0]
                date_author_info = clean_text(meta.select('.timestamp')[0].text)
                author = re.search(':([А-Яа-я\s]+$)', date_author_info)
                author = author.group(1).strip() if author is not None else None

                # 10:21                   27 декември 2019
                articleDate = ' '.join(date_author_info.split('|')[0:2]).strip()
                month_name = re.search('([а-яА-Я]+)', articleDate)
                month_name = month_name.group(1) if month_name is not None else None
                articleDate = articleDate.replace(month_name, replace_month_with_digit(month_name)) \
                    if month_name is not None else articleDate
                articleDate = pd.to_datetime(articleDate, format='%H:%M                   %d %m %Y')

                views = meta.select('#articleViews')[0].text
                comments = meta.select('.comments')[0].text
                article_text = ' '.join([par.text.strip() for par in page.select('.article-body')[0].select('p')])

                """
                window._io_config=window._io_config||{};window._io_config["0.2.0"]=window._io_config["0.2.0"]||[];window._io_config["0.2.0"].push({"page_url":"https:\/\/dnes.dir.bg\/temida\/vks-i-da-otkradnat-kolata-tryabva-da-si-plashtash-lizinga"
"page_url_canonical":"https:\/\/dnes.dir.bg\/temida\/vks-i-da-otkradnat-kolata-tryabva-da-si-plashtash-lizinga"
"page_title":"\u0412\u041a\u0421:\u0418\u0434\u0430\u043e\u0442\u043a\u0440\u0430\u0434\u043d\u0430\u0442\u043a\u043e\u043b\u0430\u0442\u0430
\u0442\u0440\u044f\u0431\u0432\u0430\u0434\u0430\u0441\u0438\u043f\u043b\u0430\u0449\u0430\u0448\u043b\u0438\u0437\u0438\u043d\u0433\u0430|\u0414\u043d\u0435\u0441.dir.bg"
"page_type":"article"
"page_language":"bg"
"article_authors":["\u041a\u0430\u043b\u0438\u043d\u041a\u0430\u043c\u0435\u043d\u043e\u0432"]
"article_categories":["\u0422\u0435\u043c\u0438\u0434\u0430"]
"article_subcategories":[]
"article_type":"image"
"article_word_count":425
"article_publication_date":"Fri
03Jan2020:52:40+0200"});
"""
                
                articles_content = articles_content.append({'link': link,
                                                            'title': clean_text(headline),
                                                            'subtitle': clean_text(subtitle),
                                                            'comments': clean_text(comments),
                                                            'author': clean_text(author),
                                                            'date': articleDate,
                                                            'views': clean_text(views),
                                                            'category': category,
                                                            'article_text': article_text},
                                                           ignore_index=True)
        except:
            continue

    return articles_content
