#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import bs4
import requests
import pandas as pd
import re
from helpers import clean_text, replace_month_with_digit


def gather_new_articles(site):
    print(site)
    request = requests.get(site)
    soup = bs4.BeautifulSoup(request.text, 'lxml')
    
    all_articles = set([art['href'] for art in soup.findAll('a', href=re.compile('^' + site + '.*' + '[\d]{7}$'))])
    articles = crawlLinks(all_articles)

    return articles


def crawlLinks(links):
    articlesContent = pd.DataFrame()

    for link in list(links):
        try:
            rq = requests.get(link)
            if rq.status_code == 200:
                page = bs4.BeautifulSoup(rq.text, 'lxml')
                
                articleTitle = page.select('h1')[0].text
                articleSubtitle = page.select('h2.subtitle')[0].text

                articleDate = page.select('.article-time')[0].text.split(', oбновена')[0]
                articleDate = clean_text(articleDate)
                month_name = re.search('([а-яА-Я]+)', articleDate)
                month_name = month_name.group(1) if month_name is not None else None
                articleDate = articleDate.replace(month_name, replace_month_with_digit(month_name)) if month_name is not None else articleDate
                articleDate = pd.to_datetime(articleDate, format='%d %m %Y,  %H:%M')

                category = page.select('div.article-category')[0].a.text
                comments = page.select('.commentsButtonNumber')[0].text
                article_text = ' '.join(
                    [clean_text(par.text) for par in page.select('.article-text')[0].select('p')])

                # article-tags
                tags = page.select('.article-tags')
                tags = ' - '.join([clean_text(tag.text) for tag in tags[0].select('a')]) if tags is not None else None
                
                articlesContent = articlesContent.append({'link': link,
                                                          'comments': clean_text(comments),
                                                          'title': clean_text(articleTitle),
                                                          'subtitle': clean_text(articleSubtitle),
                                                          'date': articleDate,
                                                          'category': category,
                                                          'tags': tags,
                                                          'article_text': article_text},
                                                         ignore_index=True)
        except:
            continue

    return articlesContent