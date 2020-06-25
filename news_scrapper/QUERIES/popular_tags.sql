WITH src as (
select 
  first_value(tags) OVER (PARTITION BY link
                    ORDER BY visited_timestamp) as tags,
  first_value(url_extract_host(link)) OVER (PARTITION BY link
                    ORDER BY visited_timestamp) as host,
  first_value(views) OVER (PARTITION BY link
                    ORDER BY visited_timestamp) as views,
  first_value(comments) OVER (PARTITION BY link
                    ORDER BY visited_timestamp) as comments
  
from raw
where tags IS NOT NULL AND tags <> ''
  )

select
lower(tag),
count(*) as tag_count,
round(avg(comments)) as avg_comments,
round(avg(views)) as avg_view 
from src
cross join unnest(split(tags, ' - ')) as t(tag)
group by 1
order by 2 desc

-----------------------
--- Daily popularity of different tags (ordered by views and comments)
-----------------------
 
CREATE TABLE IF NOT EXISTS news.popular_tags
WITH (format='parquet', external_location='s3://testo-bucket/popular-tags') as
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
	and (tags is not null or trim(tags) <> '')
  -- and url_extract_host(link) like '%.bg'
),
  
daily as (select 
	site,
	date(visited_timestamp) as date,
	split(lower(tags), ' - ') as tags,
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

tag_views as (
select 
	d.site, 
	d.date, 
	tag, 
	sum(comments_generated) as sum_comments, 
	t.site_sum_comments, 
	round(sum(comments_generated) * 100. / t.site_sum_comments, 1) as share_comments, 
	sum(views_generated) as sum_views, 
	t.site_sum_views,
	round(sum(views_generated) * 100. / t.site_sum_views, 1) as share_views
from daily d
cross join unnest(tags) as t(tag)
left join total_views t
	on d.site = t.site and d.date = t.date
group by 1, 2, 3, 5, 8
order by 1, 2, 3),

ranked as (
select
	tag_views.*,
	row_number() over (partition by date 
					   order by sum_views desc, sum_comments desc) as rnk
from tag_views
order by 1, 2, 7 desc)

select *
from ranked
