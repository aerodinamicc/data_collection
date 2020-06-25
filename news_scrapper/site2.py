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

    most_read = set([art.h2.a['href'] for art in soup.select('.additional-articles')[0].find_all('ul')[1].select('li')])
    all_articles = set([a['href'] for a in soup.select('a') if a['href'].startswith(site)])
    all_articles = all_articles.difference(most_read)

    most_read = crawlLinks(most_read)
    most_read['section'] = 'most_read'
    all_articles = crawlLinks(all_articles)
    all_articles['section'] = None

    articles = pd.concat([most_read, all_articles])

    return articles


def crawlLinks(links):
    articles_content = pd.DataFrame()

    for link in links:
        try:
            rq = requests.get(link)
            domain = "{0.netloc}".format(urlsplit(link))
            category = re.search(domain + '/([^/]+)', link).group(1)

            if rq.status_code == 200:
                page = bs4.BeautifulSoup(rq.text, 'lxml')

                headline = page.select('h1')[0].text
                author = page.select('.author')
                author = author[0].select('a')[0].text if author is not None else None

                # 30.12.2019 13:02:31
                articleDate = clean_text(page.select('.article-info')[0].select('p')[0].text)
                articleDate = pd.to_datetime(articleDate, format='%d.%m.%Y %H:%M:%S')

                views = page.select('.article-info')[0].div.p.text
                views = views.split(" ")[1]
                comments = page.select('.comments')[0].span.text
                tags = ' - '.join([clean_text(tag.text) for tag in page.select('.tags')[0].select('a') if tag != ',' and tag != "\n"])\
                    if len(page.select('.tags')) > 0 else None

                article_text = ' '.join([clean_text(par.text) for par in page.select('.article-text')[0].select('p')])

                thumbs = page.select('.rate')[0].select('a')
                thumbs_up = clean_text(thumbs[0].text)
                thumbs_down = clean_text(thumbs[1].text)

                articles_content = articles_content.append({'link': link,
                                                            'title': clean_text(headline),
                                                            'comments': clean_text(comments),
                                                            'author': clean_text(author),
                                                            'date': articleDate,
                                                            'views': clean_text(views),
                                                            'category': category,
                                                            'tags': tags,
                                                            'article_text': article_text,
                                                            'thumbs_up': thumbs_up,
                                                            'thumbs_down': thumbs_down},
                                                           ignore_index=True)
        except:
            continue

    return articles_content
