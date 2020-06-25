#!/usr/bin/env python
# -*- coding: utf-8 -*-
import bs4
import requests
import pandas as pd
import re
from urllib.parse import unquote
from helpers import clean_text, replace_month_with_digit


def gather_new_articles(site):
    request = requests.get(site)
    soup = bs4.BeautifulSoup(request.text, 'lxml')

    all_articles = set([unquote(a['href']) for a in soup.find_all('a', href=True) if
                  a['href'].startswith(site) and
                  a['href'].endswith('/') and
                  'category/' not in a['href'] and
                  'бъдете-с-nova-през-целия-ден' not in unquote(a['href'])])

    all_articles = crawlLinks(all_articles)

    return all_articles


def crawlLinks(links):
    articles_content = pd.DataFrame()

    for link in links:
        try:
            rq = requests.get(link)

            if rq.status_code == 200:
                page = bs4.BeautifulSoup(rq.text, 'lxml')
                category = page.select('.gtm-ArticleBreadcrumb-click')[0].text
                headline = page.select('.title-wrap-roboto')[0].h1.text.strip()

                # Гледайте цялата емисия
                if headline == '':
                    continue

                subtitle = page.select('.article-sub-title')[0].text.strip()
                author = page.select('.author-name')
                author = author[0].text if author is not None else None

                # 21 ноември 2019  19:42
                articleDate = page.select('.date-time')[0].text
                month_name = re.search('([а-яА-Я]+)', articleDate)
                month_name = month_name.group(1) if month_name is not None else None
                articleDate = articleDate.replace(month_name, replace_month_with_digit(
                    month_name)) if month_name is not None else articleDate
                articleDate = pd.to_datetime(articleDate, format='%d %m %Y  %H:%M')

                article_body = page.select('.article-body')[0].find_all('p', a=False)
                article_text = ' '.join([clean_text(par.text)
                                         for par in article_body if 'ГАЛЕРИЯ' not in par and
                                                                    'СНИМКИ' not in par and
                                                                    'ВИДЕО' not in par])

                #tags

                tags_start_phrase = 'w2g.targeting = '
                start_ind = rq.text.find(tags_start_phrase)
                end_ind = rq.text.find(';', start_ind)
                aoi = rq.text[start_ind + len(tags_start_phrase):end_ind].strip()
                tags = re.findall('([а-яА-Я]+)', aoi)
                tags = ' - '.join(clean_text(tag.replace("'", '').strip()) for tag in tags) if len(tags) > 0 else None

                #shares
                # shares = page.select('.inlineBlock')[1].select('.span')[-1].text

                """
                function
                getCookie(k)
                {
                return (document.cookie.match('(^|; )' + k + '=([^;]*)') | | 0)[2]
                }
                // header
                bidding
                targeting.Main
                script is loaded
                via
                GTM
                var
                w2g = w2g | | {};
                w2g.targeting = {
                cid: 'news',
                bid: 'view',
                aid: '273680',
                catid: '12',
                subcatid: '4',
                procatid: '1',
                prpage: '0',
                safe: '1',
                tag: 'тенис',
                tag: 'джейми',
                tag: 'мъри',
                tag: 'григор',
                tag: 'димитров',
                tag: 'александър',
                tag: 'лазаров',
                tag: 'великобритания',
                tag: 'българия'
            };
                """


                
                articles_content = articles_content.append({'link': link,
                                                            'title': clean_text(headline),
                                                            'subtitle': clean_text(subtitle),
                                                            'author': clean_text(author),
                                                            'date': articleDate,
                                                            'tags': tags,
                                                            #'shares': shares,
                                                            'category': category,
                                                            'article_text': article_text},
                                                           ignore_index=True)
        except:
            continue

    return articles_content
