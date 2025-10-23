
/*
Business Request – 1: Monthly Circulation Drop Check
Generate a report showing the top 3 months (2019–2024) where any city recorded the sharpest month-over-month decline in net_circulation.
Fields:city_name, month (YYYY-MM). net_circulation */

with cte_prev_mths as(
select City_ID, FORMAT(Month, 'yyyyMM') as Month , Net_Circulation, 
LAG(FORMAT(Month, 'yyyyMM')) OVER(PARTITION BY City_ID order by FORMAT(Month, 'yyyyMM')) as prev_month,
LAG(Net_Circulation,1,0) OVER(PARTITION BY City_ID order by FORMAT(Month, 'yyyyMM'))
as prev_Circulation
from fact_print_sales)
,cte_MOM_Circulations as(
select * ,
(Net_Circulation-prev_Circulation) as chg_prev_mth 
from cte_prev_mths
where (Net_Circulation-prev_Circulation) <0)
select * from(
select c.city,m.Month,m.Net_Circulation,m.prev_month, m.prev_circulation,m.chg_prev_mth,
DENSE_RANK() OVER(order by chg_prev_mth) as rnk from cte_MOM_Circulations m
INNER JOIN dim_city c ON c.city_id = m.City_ID) b where b.rnk <=3

/*
Business Request – 2: Yearly Revenue Concentration by Category
Identify ad categories that contributed > 50% of total yearly ad revenue.
Fields: year,category_name,category_revenue,total_revenue_year,pct_of_year_total */

with cte_ad_revenue_INR as(
select * , case when currency = 'USD' then 88.17 * ad_revenue 
				when currency = 'EUR' then 103.29*ad_revenue
                else ad_revenue end as ad_revenue_INR from fact_ad_revenue
)

, cte_category_revenue as(
select SUBSTRING(Quarter,1,4) as Year, ad_category as category_name, ROUND(sum(ad_revenue_INR),2) as category_revenue 
from cte_ad_revenue_INR
group by SUBSTRING(Quarter,1,4), ad_category)

,cte_yearly_revenue as(
select *, SUM(category_revenue) OVER(PARTITION BY Year) as total_revenue_year from cte_category_revenue)

select year,a.standard_ad_category,c.category_name,c.total_revenue_year, ROUND((category_revenue/total_revenue_year)*100.0,2) as pct_of_year_total 
from cte_yearly_revenue c 
INNER JOIN dim_ad_category a on c.category_name = a.ad_category_id;


/*
Business Request – 3: 2024 Print Efficiency Leaderboard
For 2024, rank cities by print efficiency = net_circulation / copies_printed. Return top 5.
Fields: city_name ,copies_printed_2024,net_circulation_2024 ,efficiency_ratio = net_circulation_2024 / copies_printed_2024 ,efficiency_rank_2024*/

with cte_printed_copies_2024 as(
select City_ID,
SUM(case when year(Month) = 2024 then (Copies_Sold+copies_returned)end) as copies_printed_2024,
SUM(case when year(Month) = 2024 then Net_Circulation end) as net_circulation_2024
from fact_print_sales
group by City_ID)
,cte_efficiency_ratio as(
select *,CAST(100.0*(net_circulation_2024*1.0/copies_printed_2024*1.0) as DECIMAL(5,2)) as efficiency_ratio from cte_printed_copies_2024)

select * from (
select c.city,e.copies_printed_2024,e.net_circulation_2024,e.efficiency_ratio, DENSE_RANK() OVER(order by efficiency_ratio desc) as efficiency_rank_2024 
from cte_efficiency_ratio e
INNER JOIN dim_city c ON c.city_id = e.City_ID
) b where b.efficiency_rank_2024<=5

/*
Business Request – 4 : Internet Readiness Growth (2021)
For each city, compute the change in internet penetration from Q1-2021 to Q4-2021 and identify the city with the highest improvement.
Fields: city_name, internet_rate_q1_2021,internet_rate_q4_2021, delta_internet_rate = internet_rate_q4_2021 − internet_rate_q1_2021 */

with cte_internet_rate as(
select city_id, CAST(SUM(case when quarter = '2021-Q1' then internet_penetration end) as DECIMAL(5,2)) as internet_rate_q1_2021,
CAST(SUM( case when quarter = '2021-Q4' then internet_penetration end) as DECIMAL(5,2)) as internet_rate_q4_2021
from fact_city_readiness
group by city_id)

select c.city,i.internet_rate_q1_2021,i.internet_rate_q4_2021 , (internet_rate_q4_2021-internet_rate_q1_2021) as delta_internet_rate from cte_internet_rate i 
INNER JOIN dim_city c on i.city_id = c.city_id
order by (internet_rate_q4_2021-internet_rate_q1_2021) desc




/*
Business Request – 5: Consistent Multi-Year Decline (2019→2024)
Find cities where both net_circulation and ad_revenue decreased every year from 2019 through 2024 (strictly decreasing sequences).
Fields: city_name,year,yearly_net_circulation,yearly_ad_revenue,is_declining_print (Yes/No per city over 2019–2024)
,is_declining_ad_revenue (Yes/No)
,is_declining_both (Yes/No) */


with cte_yearly_metrics as(
select s.City_ID as city_id, YEAR(s.Month) as Year, SUM(Net_Circulation) as yearly_net_circulation,
ROUND(SUM(ar.ad_revenue),2) as yearly_ad_revenue from fact_print_sales s INNER JOIN 
fact_ad_revenue ar ON s.edition_ID = ar.edition_id and YEAR(s.Month) = SUBSTRING(ar.Quarter,1,4)
group by s.City_ID, YEAR(s.Month))

,cte_prev_yr_metrics as(
select *, LAG(yearly_net_circulation) OVER(PARTITION BY city_id order by year) as prev_yr_circulation ,
LAG(yearly_ad_revenue) OVER(PARTITION BY city_id order by year) as prev_yr_ad_revenue
from cte_yearly_metrics)

,cte_is_decline as(
select city_id, year ,
CASE when yearly_net_circulation < prev_yr_circulation then 'Yes' else 'No' end as is_declining_print,
CASE when yearly_ad_revenue < prev_yr_ad_revenue then 'Yes' else 'No' end as is_declining_ad_revenue
from cte_prev_yr_metrics)

,cte_final_result as(
select * , case when is_declining_print = 'Yes' and is_declining_ad_revenue= 'Yes' then 'Yes' else 'No' end as is_declining_both
from cte_is_decline)

--select * from cte_final_result

select c.city, STRING_AGG( case when is_declining_both='Yes' then year else NULL end,',') as declining_years,
sum( case when is_declining_both = 'Yes' then 1 else 0 end ) as   no_years_declined from cte_final_result f 
INNER JOIN dim_city c on f.city_id = c.city_id
group by city;


/*
Business Request – 6 : 2021 Readiness vs Pilot Engagement Outlier
In 2021, identify the city with the highest digital readiness score but among the bottom 3 in digital pilot engagement.
readiness_score = AVG(smartphone_rate, internet_rate, literacy_rate)
“Bottom 3 engagement” uses the chosen engagement metric provided (e.g., engagement_rate, active_users, or sessions).
Fields:
city_name,readiness_score_2021,engagement_metric_2021,readiness_rank_desc,engagement_rank_asc,is_outlier (Yes/No) */

with cte_readiness_score as(
select city_id, quarter, CAST((literacy_rate+smartphone_penetration+internet_penetration)/3 AS DECIMAL(5,2)) as readiness_score
from fact_city_readiness)
,cte_score_2021 as(
select city_id, AVG(case when SUBSTRING(quarter,1,4) = 2021 then readiness_score else NULL end) as readiness_score_2021 
from cte_readiness_score
group by city_id)

,cte_engagement_metric as(
select city_id, SUBSTRING(launch_month,1,4) as Year, SUM( downloads_or_accesses) as engagement_metric_2021  from fact_digital_pilot
group by city_id, SUBSTRING(launch_month,1,4))

,cte_ranks as(
select c.city,s.readiness_score_2021,e.engagement_metric_2021,
DENSE_RANK() OVER(order by s.readiness_score_2021 desc) as readiness_rnk_desc,
DENSE_RANK() OVER(order by e.engagement_metric_2021) as engagement_rnk
from cte_score_2021 s INNER JOIN cte_engagement_metric e on s.city_id = e.city_id
INNER JOIN dim_city c on e.city_id = c.city_id
)

--select * from cte_ranks

select * , case when readiness_rnk_desc in (1,2,3) and engagement_rnk in (1,2,3) then 'Yes' else 'No' end as is_outlier
from cte_ranks
