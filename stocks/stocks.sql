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

select sector, avg(percent_lower) as avg_percent_lower, count(*) as count_
from (
	select 
		*,
		ROUND(100 - ((high/max_ever) * 100.), 2) as percent_lower
	from (
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
	) v
	where at_date = '2021-08-06'::date
) d
group by 1
order by 2 desc
;