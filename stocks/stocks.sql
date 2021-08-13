select symbol from sp500_companies sc where details is null;


select 
	sector, 
	sum(market_cap_mln) as sum_market_cap_mln, 
	count(*) as companies_count, 
	avg(market_cap_mln) as avg_market_cap_mln,
	sum(weight) as sum_weight, 
	avg(weight) as avg_weight
from
(
select 
	*, 
	details->>'sector' as sector,
	round(cast(details->>'marketCap' as numeric) / 1000000) as market_cap_mln
from sp500_companies 
where details->>'marketCap' is not null
order by weight desc
) v
group by 1
order by 2 desc;


----
-- round function
----

CREATE FUNCTION ROUND(float,int) RETURNS NUMERIC AS $$
   SELECT ROUND($1::numeric,$2);
$$ language SQL IMMUTABLE;

------
-- Price below historic prices averaged across sectors
------

with src as (
select 
	sc.name, 
	sc.sector,
	pa.symbol, 
	pa.at_date,
	pa.volume, 
	pa.high, 
	max(high) over (partition by symbol) as max_ever
from price_actions pa
left join (select *, details->>'sector' as sector from sp500_companies) sc using(symbol)
), 
perc as (
select 
	*,
	ROUND(100 - ((high/max_ever) * 100.), 2) as percent_lower
from src 
where at_date = '2021-08-06'::date 
	OR high = max_ever
),
mid as (
select 
 p1.*,
 date_part('day', '2021-08-06'::timestamp - coalesce(p2.at_date, p1.at_date)::timestamp) as days_since_max_date,
 coalesce(p2.at_date, p1.at_date) as max_date
from perc p1
left join perc p2 using(symbol)
where p1.at_date = '2021-08-06'::date
and p2.at_date <> '2021-08-06'::date
),
fin as (
select 
	sector, 
	count(*) as count_,
	round(avg(percent_lower)) as avg_percent_lower, 
	round(stddev(percent_lower), 2) as std_percent_lower,
	round(avg(days_since_max_date)) as avg_days_since_max_date,
	round(stddev(days_since_max_date), 2) as std_days_since_max_date
from mid
where sector is not null
group by 1
order by 3 desc
)
select * 
--into ticker_stats from mid
from fin;

select * 
from ticker_stats 
where sector = 'Consumer Cyclical'

------
-- share of total volume per sector per month
------ 

with src as (
select 
	sc.sector,
	date_trunc('month', at_date) as at_date,
	--To_char(at_date, 'IYYY-IW') as at_date,
	sum(pa.volume) as sum_volume
from price_actions pa
left join (select *, details->>'sector' as sector from sp500_companies) sc using(symbol)
group by 1, 2
),
share_ as (
select 
	*,
	round((sum_volume / sum(sum_volume) over (partition by at_date)) * 100, 2) as percent_from_all_volume,
	sum(sum_volume) over (partition by at_date) as volume_that_day
from src
)
select 
	sector, at_date, sum_volume, percent_from_all_volume, 
	percent_from_all_volume - lag(percent_from_all_volume) over (partition by sector order by at_date) as delta_perc_from_all_volume
from share_
where date_part('year', at_date) = 2021
--where left(at_date, 4) = '2021'
and sector is not null
order by 2, 1
