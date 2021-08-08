select symbol from sp500_companies sc where details is null;

select 
	*, 
	details->>'sector' as sector,
	round(cast(details->>'marketCap' as numeric) / 1000000) as market_cap_mln
from sp500_companies 
where details->>'marketCap' is not null
order by weight desc;