CREATE TABLE IF NOT EXISTS news.popular_articles
WITH (format='parquet', external_location='s3://testo-bucket/popular-articles') as
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
	trim(tags) as tags,
	category,
	row_number() OVER (PARTITION BY link
					   ORDER BY visited_timestamp) as rnk,
	partition_0 as site
from news.raw
	where substr(created_timestamp, 1, 10) like '2020-%'
),
  
daily as (select 
	site,
	date(visited_timestamp) as date,
	link,
	title,
	tags,
	case when rnk = 1 then comments
	     else comments - prev_comments END AS comments_generated,
	case when rnk = 1 then views
	     else views - prev_views END AS views_generated
from src),

total_views as (
select 
	site, 
	date, 
	sum(comments_generated) as site_sum_comments, 
	sum(views_generated) as site_sum_views
from daily
group by 1, 2
),

articles_views as (
select 
	d.site, 
	d.date, 
	link, 
	title,
	tags,
	sum(comments_generated) as sum_comments, 
	t.site_sum_comments, 
	round(sum(comments_generated) * 100. / t.site_sum_comments, 1) as share_comments, 
	sum(views_generated) as sum_views, 
	t.site_sum_views,
	round(sum(views_generated) * 100. / t.site_sum_views, 1) as share_views
from daily d
left join total_views t
	on d.site = t.site and d.date = t.date
group by 1, 2, 3, 4, 5, 7, 10
order by 1, 2, 3, 4, 5)

select * from articles_views
	    
------------------------------------------------
--- Articles that generated more than 10 % of their site's traffic for a specofic date
------------------------------------------------
select
	site, 
	date, 
	sum_comments, 
	sum_views, 
	site_sum_views, 
	share_views, 
	title 
from news.popular_articles
	where share_views > 10
	order by site, date
