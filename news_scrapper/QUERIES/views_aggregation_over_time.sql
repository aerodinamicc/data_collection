------
--Get articles by domain per day
------

select regexp_extract(url_extract_host(link), '([\w]+.bg)$', 1), substr(created_timestamp, 1, 10), count(*)
from news.raw
where substr(created_timestamp, 1, 10) like '2020-%'
group by 1, 2
order by 1, 2

------
--Compute views and comment rates
------

WITH src as (
select
  link,
  title,
  views,
  comments,
  date_parse(visited_timestamp, '%Y-%m-%d %H:%i:%S') as visited_timestamp,
  date_parse(created_timestamp, '%Y-%m-%d %H:%i:%S') as created_timestamp,
  lag(comments) OVER (PARTITION BY link
                    ORDER BY visited_timestamp) as prev_comments,
  lag(views) OVER (PARTITION BY link
                    ORDER BY visited_timestamp) as prev_views,
  lag(date_parse(visited_timestamp, '%Y-%m-%d %H:%i:%S')) OVER (PARTITION BY link
                    ORDER BY visited_timestamp) as prev_visited,
  tags,
  category,
  
  row_number() OVER (PARTITION BY link
                    ORDER BY visited_timestamp) as rnk
  from news.raw
  where substr(created_timestamp, 1, 10) like '2020-%'
  and url_extract_host(link) like '%sportal.bg'
)
  
select 
  link,
  title,
  visited_timestamp,
  prev_visited,
case when rnk = 1 then date_diff('minute', created_timestamp, visited_timestamp)
     else date_diff('minute', prev_visited, visited_timestamp) END AS time_passed,
     comments,
case when rnk = 1 then comments
     else comments - prev_comments END AS comments_generated,
     views,
case when rnk = 1 then views
     else views - prev_views END AS views_generated
from src
