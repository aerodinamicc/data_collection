--------------------------
---- Get daily aggregated views per site
--------------------------
	    
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
	row_number() OVER (PARTITION BY link
					   ORDER BY visited_timestamp) as rnk,
	partition_0 as site
from news.raw
	where substr(created_timestamp, 1, 10) like '2020-%'
)
select 
	site,
	date(visited_timestamp) as date,
	SUM(case when rnk = 1 then comments
	     else comments - prev_comments end) AS sum_comments,
	SUM(case when rnk = 1 then views
	     else views - prev_views end) AS sum_views
from src
group by 1, 2
order by 1, 2
