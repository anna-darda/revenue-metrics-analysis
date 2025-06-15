-- Description : SQL script to compute product revenue metrics for dashboard visualization
-- Project     : Revenue Metrics Dashboard
-- Author      : Anna Darda
-- Date        : June 2025

-- CTE: Base data with payment and user info, including first/last payment dates
with cte_base_data as (
  select 
    gp.user_id
    , gp.payment_date 
    , date_trunc('month', gp.payment_date) as payment_month 
    , min(gp.payment_date) over(partition by gp.user_id) as first_payment_date
    , date_trunc('month', min(gp.payment_date) over(partition by gp.user_id)) as first_payment_month
    , date_trunc('month', max(gp.payment_date) over (partition by gp.user_id)) as last_payment_month
    , gp.revenue_amount_usd
    , gpu.language
    , gpu.age
from public.games_payments as gp
left join public.games_paid_users as gpu on gp.user_id = gpu.user_id
),
-- CTE: Monthly user activity with flags for new users and new MRR
cte_monthly_user_activity as (
select 
	cbd.user_id 
	, cbd.payment_month
	, cbd.revenue_amount_usd
	, lag(cbd.revenue_amount_usd) over (partition by cbd.user_id order by cbd.payment_month) as prev_revenue
	, case 
		when cbd.payment_month = cbd.first_payment_month then true 
  		else false 
	end as is_new_user
	, case 
  		when cbd.payment_month = cbd.first_payment_month then cbd.revenue_amount_usd
  		else 0 
	end as new_mrr
	, cbd.language  
	, cbd.age
from cte_base_data as cbd
),
-- CTE: Calculates monthly revenue per user with future payment indicators
cte_user_monthly_revenue as (
  select
    user_id
  , payment_month
  , language
  , age
  , sum(revenue_amount_usd) as total_revenue
  , payment_month + interval '1 month' as next_calendar_month
  , lead(payment_month) over (partition by user_id order by payment_month) as next_paid_month
  from cte_base_data
  group by 
    user_id
  , payment_month
  , language
  , age
),
-- CTE: Churn metrics including churned users and churned revenue
cte_churn_block as (
select
	count(distinct user_id) as paid_users
	, payment_month
	, payment_month + interval '1 month' as churn_month
	, language  
	, age
	, sum(total_revenue) as mrr
	, count(
		case 
			when next_paid_month is null
				or next_paid_month != next_calendar_month 
			then 1 
		end
	) as churn_user
    , sum(
    	case 
	    	when next_paid_month is null
	    		or next_paid_month != next_calendar_month 
	    	then total_revenue 
	    end
	) as churn_revenue
from cte_user_monthly_revenue
group by
	payment_month
	, language 
	, age
),
-- CTE: Lagged values for previous month's paid users and MRR
cte_churn_lag_block as (
select
	ccb.payment_month
	, ccb.churn_month
	, ccb.language 
	, ccb.age
	, ccb.paid_users
	, ccb.mrr
	, ccb.churn_user
	, ccb.churn_revenue
	, lag(ccb.paid_users) over (partition by ccb.language, ccb.age order by ccb.payment_month) as paid_users_prev
	, lag(ccb.mrr) over (partition by ccb.language, ccb.age order by ccb.payment_month) as mrr_prev
from cte_churn_block as ccb
),
-- CTE: Calculates expansion and contraction MRR based on revenue changes
cte_expansion_contraction as (
	select
		um.payment_month
		, um.language
		, um.age
		, sum(
			case
				when um.monthly_revenue > um.prev_monthly_revenue 
					then um.monthly_revenue - um.prev_monthly_revenue
				else 0
			end
		) as expansion_mrr
		, sum(
			case
				when um.monthly_revenue < um.prev_monthly_revenue 
					then um.monthly_revenue - um.prev_monthly_revenue
				else 0
			end
		) as contraction_mrr
	from (
		select
			cumr.user_id
			, cumr.payment_month
			, cumr.language
			, cumr.age
			, cumr.total_revenue as monthly_revenue
			, lag(cumr.total_revenue) over (
				partition by cumr.user_id 
				order by cumr.payment_month
			) as prev_monthly_revenue
		from cte_user_monthly_revenue cumr
	) as um
	group by
		um.payment_month
		, um.language
		, um.age
),
-- CTE: LTV and lifetime calculation per user
cte_user_ltv as (
select
	cbd.user_id
	, cbd.language  
	, cbd.age
	, sum(revenue_amount_usd) as ltv
	, extract(epoch from(cbd.last_payment_month - cbd.first_payment_month)) / 2629800 as lifetime
from	
	cte_base_data as cbd
group by
	cbd.user_id
	, cbd.language  
	, cbd.age
	, cbd.last_payment_month
	, cbd.first_payment_month
),
-- CTE: Average LTV and lifetime per month/language/age group
cte_ltv_lifetime as (
select
	cbd.payment_month
	, cbd.language  
	, cbd.age
	, avg(cul.ltv) as avg_ltv
	, avg(cul.lifetime) as avg_lifetime
from	
	cte_base_data as cbd
left join cte_user_ltv as cul on cbd.user_id = cul.user_id
group by
	cbd.payment_month
	, cbd.language  
	, cbd.age	
),
-- CTE: Final metrics combining all previous computations
cte_final_metrics as (
select
	cclb.payment_month
	, cclb.churn_month
	, cclb.language
	, cclb.age
	, cclb.paid_users
	, cclb.paid_users_prev
	, round(cclb.mrr::numeric,2) as mrr
	, round(cclb.mrr_prev::numeric,2) as mrr_prev
	, cclb.churn_user
	, round(cclb.churn_revenue::numeric,2) as churn_revenue
	, round(cclb.churn_user::numeric/nullif(paid_users_prev::numeric, 0),2) as churn_rate
	, round(cclb.churn_revenue::numeric/nullif(mrr_prev::numeric, 0),2) as revenue_churn_rate
	, npu.new_paid_users
	, round(npu.total_new_mrr::numeric,2) as total_new_mrr
	, round(cec.expansion_mrr::numeric,2) as expansion_mrr
	, round(cec.contraction_mrr::numeric,2) as contraction_mrr
	, round((npu.total_new_mrr + cec.expansion_mrr - cclb.churn_revenue - cec.contraction_mrr)::numeric, 2) as net_mrr
	, round(cll.avg_ltv::numeric,2) as avg_ltv
	, round(cll.avg_lifetime::numeric,2) as avg_lifetime
	, round(cclb.mrr::numeric / nullif(cclb.paid_users, 0),2) as arppu
from cte_churn_lag_block as cclb
left join (
  select
    payment_month
    , language
    , age
    , count(distinct case when is_new_user then user_id end) as new_paid_users
    , sum(new_mrr) as total_new_mrr
  from cte_monthly_user_activity
  group by 
  	payment_month
  	, language
  	, age
) as npu
on cclb.payment_month = npu.payment_month
  and cclb.language = npu.language
  and cclb.age = npu.age

left join cte_expansion_contraction as cec on cclb.payment_month = cec.payment_month
  and cclb.language = cec.language
  and cclb.age = cec.age
left join cte_ltv_lifetime as cll on cclb.payment_month = cll.payment_month
  and cclb.language = cll.language
  and cclb.age = cll.age
)
-- Final SELECT: Output all calculated metrics
select 
	payment_month
	, churn_month
	, language
	, age
	, paid_users
	, paid_users_prev
	, mrr
	, mrr_prev
	, churn_user
	, churn_revenue
	, churn_rate
	, revenue_churn_rate
	, new_paid_users
	, total_new_mrr
	, expansion_mrr
	, contraction_mrr
	, net_mrr
	, avg_ltv
	, avg_lifetime
	, arppu
from cte_final_metrics 	

	



