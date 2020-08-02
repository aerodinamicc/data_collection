#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import bs4
import requests
import pandas as pd
import re
from tqdm import tqdm
from helpers import clean_text, replace_month_with_digit


def gather_new_articles(site):
    request = requests.get(site)
    soup = bs4.BeautifulSoup(request.text, features="html.parser")
    
    arts = soup.findAll('a', href=re.compile('^' + site + '.*' + '[\d]{7}$'))
    all_articles = set([(art['href'], re.search('gtm-(.*)-click', art['class'][0]).group(1)) for art in arts])
    articles = crawlLinks(all_articles)

    return articles


def crawlLinks(links):
    articlesContent = pd.DataFrame()

    for link, section in tqdm(list(links)):
        try:
            rq = requests.get(link)
            if rq.status_code == 200:
                page = bs4.BeautifulSoup(rq.text, features="html.parser")
                
                articleTitle = page.select('h1')[0].text if len(page.select('h1')) > 0 else ''
                articleSubtitle = page.select('h2.subtitle')[0].text if len(page.select('h2.subtitle')) > 0 else ''

                articleDate = page.select('.article-time')[0].text.split(', oбновена')[0] if len(page.select('.article-time')) > 0 else ''
                articleDate = clean_text(articleDate)
                month_name = re.search('([а-яА-Я]+)', articleDate)
                if month_name is not None:
                    month_name = month_name.group(1)
                    articleDate = articleDate.replace(month_name, replace_month_with_digit(month_name))
                    articleDate = pd.to_datetime(articleDate, format='%d %m %Y,  %H:%M')

                category = page.select('div.article-category')[0].a.text if len(page.select('div.article-category')) > 0 else ''
                comments = page.select('.commentsButtonNumber')[0].text if len(page.select('.commentsButtonNumber')) > 0 else ''
                article_text = ' '.join(
                    [clean_text(par.text) for par in page.select('.article-text')[0].select('p')])

                # article-tags
                tags = page.select('.article-tags')
                tags = ' - '.join([clean_text(tag.text) for tag in tags[0].select('a')]) if tags is not None else None
                
                articlesContent = articlesContent.append({'link': link,
                                                          'section': section,
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