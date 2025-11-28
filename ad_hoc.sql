-- Business Request – 1: Monthly Circulation Drop Check 
-- Generate a report showing the top 3 months (2019–2024) where any city recorded the 
-- sharpest month-over-month decline in net_circulation. 

with cte1 as (select c.city_id,
				c.city as city_name,
				p.Month as Month,
				p.Net_Circulation as net_circulation,
				lag(Net_Circulation) over (partition by c.city_id order by p.Month) as prev_net_circulation
from fact_print_sales p 
join dim_city c 
on c.city_id = p.City_ID)
select city_name,Month,net_circulation,
		prev_net_circulation,
		(net_circulation - prev_net_circulation) as MOM_change
from cte1
where prev_net_circulation is not null 
order by  MOM_change ,Month
limit 3
;


-- Business Request – 2: Yearly Revenue Concentration by Category 
-- Identify ad categories that contributed > 50% of total yearly ad revenue. 

with cte1 as (select r.year,c.ad_category_id,c.standard_ad_category ,
sum(r.ad_revenue_inr) as category_ad_revenue
from fact_ad_revenue r 
join dim_ad_category1 c 
on r.ad_category = c.ad_category_id
group by r.year , c.standard_ad_category,c.ad_category_id ),
cte2 as (select year,sum(category_ad_revenue) as total_revenue_year 
from cte1
group by year)
select e1.year,e1.ad_category_id,e1.standard_ad_category,e1.category_ad_revenue,e2.total_revenue_year,
round((e1.category_ad_revenue/e2.total_revenue_year)*100,2) as pct_of_total_year
from cte1 e1
join cte2 e2
on e1.year = e2.year
where (e1.category_ad_revenue*100/e2.total_revenue_year)
order by e1.year,pct_of_total_year desc;


-- Business Request – 3: 2024 Print Efficiency Leaderboard 
-- For 2024, rank cities by print efficiency = net_circulation / copies_printed. Return top 5
-- Explanation:-copies_printed = sum(copies_sold)
-- 				net_circulataion=sum(copies_sold-copies_returned).


with cte as (select c.city , 
sum(case when p.year = 2024 then p.Copies_Sold end ) as Copies_sold_2024,
sum(case when p.year = 2024 then p.net_circulation end ) as Net_circulation_2024

from dim_city c 
join fact_print_sales p 
on c.city_id = p.City_ID 
group by c.city)
select city,Copies_sold_2024,Net_circulation_2024,
       ROUND(Net_circulation_2024/NULLIF(Copies_sold_2024,0)*100,2) AS efficiency_ratio,
       DENSE_RANK() OVER (ORDER BY Net_circulation_2024/NULLIF(Copies_sold_2024,0) DESC) AS efficiency_rank_2024
FROM cte
WHERE Copies_sold_2024 > 0
ORDER BY efficiency_rank_2024
LIMIT 5;




-- Business Request – 4 : Internet Readiness Growth (2021) 
-- For each city, compute the change in internet penetration from Q1-2021 to Q4-2021 
-- and identify the city with the highest improvement. 

SELECT
    t1.city,
    MAX(CASE WHEN t2.quarter = 'Q1' AND t2.Year = '2021' THEN t2.internet_penetration ELSE NULL END) AS internet_rate_q1_2021,
    MAX(CASE WHEN t2.quarter = 'Q4' AND t2.Year = '2021' THEN t2.internet_penetration ELSE NULL END) AS internet_rate_q4_2021,
    round((MAX(CASE WHEN t2.quarter = 'Q4' AND t2.Year = '2021' THEN t2.internet_penetration ELSE NULL END) - 
     MAX(CASE WHEN t2.quarter = 'Q1' AND t2.Year = '2021' THEN t2.internet_penetration ELSE NULL END)),2) AS delta_internet_rate
FROM
    dim_city AS t1
JOIN
    fact_city_readiness AS t2 ON t1.city_id = t2.city_id
GROUP BY
    t1.city
ORDER BY
    delta_internet_rate DESC;



-- Business Request – 5: Consistent Multi-Year Decline (2019→2024) 
-- Find cities where both net_circulation and ad_revenue decreased every year from 2019 
-- through 2024 (strictly decreasing sequences). 

WITH yearly_data AS (
    SELECT 
        c.city as city_name,
        fps.year,
        SUM(fps.Net_Circulation) AS yearly_net_circulation,
        round(SUM(far.ad_revenue_inr),2) AS yearly_ad_revenue
    FROM fact_print_sales fps
    JOIN dim_city c ON fps.city_id = c.city_id
    JOIN fact_ad_revenue far ON fps.edition_id = far.edition_id AND fps.year = far.year
    WHERE fps.year BETWEEN "2019" AND "2024"
    GROUP BY c.city, fps.year
),

check_decline AS (
    SELECT 
        city_name,
        year,
        yearly_net_circulation,
        yearly_ad_revenue,
        LAG(yearly_net_circulation) OVER (PARTITION BY city_name ORDER BY year) AS prev_net_circulation,
        LAG(yearly_ad_revenue) OVER (PARTITION BY city_name ORDER BY year) AS prev_ad_revenue
    FROM yearly_data
)

SELECT 
    city_name,
    year,
    yearly_net_circulation,
    yearly_ad_revenue,
    CASE 
        WHEN SUM(CASE WHEN yearly_net_circulation < prev_net_circulation THEN 1 ELSE 0 END) 
             = COUNT(prev_net_circulation) 
        THEN 'Yes' ELSE 'No' 
    END AS is_declining_print,
    CASE 
        WHEN SUM(CASE WHEN yearly_ad_revenue < prev_ad_revenue THEN 1 ELSE 0 END) 
             = COUNT(prev_ad_revenue) 
        THEN 'Yes' ELSE 'No' 
    END AS is_declining_ad_revenue,
    CASE 
        WHEN SUM(CASE WHEN yearly_net_circulation < prev_net_circulation THEN 1 ELSE 0 END) 
             = COUNT(prev_net_circulation)
         AND SUM(CASE WHEN yearly_ad_revenue < prev_ad_revenue THEN 1 ELSE 0 END) 
             = COUNT(prev_ad_revenue)
        THEN 'Yes' ELSE 'No' 
    END AS is_declining_both
FROM check_decline
GROUP BY city_name, year,
    yearly_net_circulation,
    yearly_ad_revenue;

-- Business Request – 6 : 2021 Readiness vs Pilot Engagement Outlier 
-- In 2021, identify the city with the highest digital readiness score but among the bottom 3 
-- in digital pilot engagement. 
-- readiness_score = AVG(smartphone_rate, internet_rate, literacy_rate) 
-- “Bottom 3 engagement” uses the chosen engagement metric provided (e.g., 
-- engagement_rate, active_users, or sessions). 



WITH readiness AS (
    SELECT 
        c.city,
        round(AVG(cr.literacy_rate + cr.smartphone_penetration + cr.internet_penetration)/3,2)
        AS readiness_score
    FROM fact_city_readiness cr
    JOIN dim_city c ON cr.city_id = c.city_id
    WHERE cr.year = 2021
    GROUP BY c.city
),
engagement AS (
    SELECT 
        c.city,
        COALESCE(SUM(dp.downloads_or_accesses),0) AS engagement_metric
    FROM fact_digital_pilot dp
    JOIN dim_city c ON c.city_id = dp.city_id
   
    GROUP BY c.city
)
SELECT 
    r.city,
    r.readiness_score,
    e.engagement_metric,
    RANK() OVER (ORDER BY r.readiness_score DESC) AS readiness_rank_desc,
    RANK() OVER (ORDER BY e.engagement_metric ASC) AS engagement_rank_asc,
    CASE 
       WHEN RANK() OVER (ORDER BY r.readiness_score DESC) = 1
        AND RANK() OVER (ORDER BY e.engagement_metric ASC) <= 3 
       THEN 'Yes' ELSE 'No' END AS is_outlier
FROM readiness r
JOIN engagement e ON r.city = e.city;
