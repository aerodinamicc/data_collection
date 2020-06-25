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

    all_articles = set([site + unquote(a['href']) for a in soup.find_all('a', href=True, title=True)
                        if '?ref' in a['href'] and
                        'author' not in a['href'] and
                        not a['href'].startswith('/?')])

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

                headline = page.select('.content')[0].h1.text.strip()
                meta = clean_text(page.select('.article-tools')[0].text)

                # 14:41, 30 дек 19
                articleDate = re.search('(.*),', meta).group(1)
                month_name = re.search('([а-яА-Я]+)', articleDate)
                month_name = month_name.group(1) if month_name is not None else None
                articleDate = articleDate.replace(month_name, replace_month_with_digit(
                    month_name)) if month_name is not None else articleDate
                articleDate = pd.to_datetime(articleDate, format='%H:%M, %d %m %y')

                views = re.search('(\d+)$', meta).group(1)
                comments = page.select('.comments')[0].text.strip()
                article_body = page.select('.article-content')[0].select('p')
                author = article_body[0].text
                article_text = ' '.join([clean_text(par.text)
                                         for par in article_body[1:] if '<' not in par.text])
                article_text = article_text[article_text.find('}') + 1:].strip()

                tags = ' - '.join(
                    [clean_text(tag.text) for tag in page.select('.tags')[0].select('li') if tag != ',' and tag != "\n"]) \
                    if len(page.select('.tags')) > 0 else None
                tags = clean_text(tags)
                
                articles_content = articles_content.append({'link': link,
                                                            'title': clean_text(headline),
                                                            'author': clean_text(author),
                                                            'date': articleDate,
                                                            'category': category,
                                                            'comments': clean_text(comments),
                                                            'views': clean_text(views),
                                                            'tags': tags,
                                                            'article_text': article_text},
                                                           ignore_index=True)
        except:
            continue

    return articles_content
