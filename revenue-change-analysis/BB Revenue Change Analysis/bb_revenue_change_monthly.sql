create or replace table `skyuk-uk-per-sub-economic-poc.ML.accounts_dates`
as

with dt_array as (
select      *
from unnest(generate_date_array('2018-10-01',current_date(), interval 1 month)) as period_start
)

,dates as (
select      period_start
            ,last_day(period_start,month) as event_dt
from        dt_array
where       last_day(period_start,month) <= current_date()        
)

,accounts_bb as (
select      distinct
            account_number
            ,portfolio_id
from        dates dt         
inner join `skyuk-uk-per-sub-economic-poc.spine_replicas.mart_subscription_broadband` msd
on          dt.event_dt < cast(msd.effective_to_dt as date)
and         dt.event_dt >= cast(msd.effective_from_dt as date)
and         date(msd.effective_to_dt) <> date(msd.effective_from_dt)
where       msd.sub_type in ('Broadband DSL Line') or ((lower(msd.sub_type) like '%boost%'
or          upper(msd.sub_type) = 'WIFI_RELATED_SUBS'))
and         msd.status_code in ('AC', 'AB', 'PC', 'PT', 'CF', 'BCRQ')
and         msd.catalogue_product_type = 'RC'
and         msd.currency_code = 'GBP'
)

,accounts_talk as (
select      distinct
            account_number
            ,portfolio_id
from        dates dt         
inner join `skyuk-uk-per-sub-economic-poc.spine_replicas.mart_subscription_talk` mst
on          dt.event_dt < cast(mst.effective_to_dt as date)
and         dt.event_dt >= cast(mst.effective_from_dt as date)
and         date(mst.effective_to_dt) <> date(mst.effective_from_dt)
where       mst.status_code in ('RI', 'PR', 'PC', 'FBP', 'FBI', 'BCRQ', 'A', -- talk status codes
                              'R', 'CRQ') -- lr status codes
and         mst.catalogue_product_type = 'RC'
and         mst.currency_code = 'GBP'
)

,accounts_all as (
select *
from accounts_bb
union all
select *
from accounts_talk
)

,accounts as (
select distinct account_number
            ,portfolio_id
from   accounts_all
)

, prep as (
select      dates.*
            ,accounts.*
from        accounts
cross join  dates
)

select prep.*
,case currency_code when 'GBP' then 'UK' when 'EURO' then 'ROI'
                  else 'Unknown' end as country
from prep
left join   `skyuk-uk-customer-pres-prod.uk_pub_cust_spine_account_is.fact_account`
using(account_number, portfolio_id)
;

create or replace table `skyuk-uk-per-sub-economic-poc.Revenue_Change_Analysis.BB_Base_Monthly`
as



-- The changes are mainly added here.
-- Change 1: CTE accounts_and_dates is adding the talk only and lr only customers
-- Change 2: CTE holding all, add condtions for rack_rate and offers, updated the status_code we had in EVT.
-- Change 3: In all CTE, change the join condtion using 'between' with '>='


with accounts_and_dates as (
      select *
      from `skyuk-uk-per-sub-economic-poc.ML.bb_base_monthly_accounts_and_dates`
--       where account_number in ('630091847826', '623941212614', '624421223899', '634242696835', '624272677706'  --bb
-- , '624159043097', '210161408997', '624592873597', '622691874193', '620011284333' --boost
-- , '210050647671', '624732891277', '624417407548', '622912631968', '623636182148' --talk
-- ,'630215049788', '210054059378', '623805385605', '620021707125', '620022913516' -- lr
)


, bb_talk as (
select     * 
from        `skyuk-uk-customer-pres-prod.uk_pub_cust_spine_subs_is.mart_subscription_broadband`
union all 
select      *
from        `skyuk-uk-customer-pres-prod.uk_pub_cust_spine_subs_is.mart_subscription_talk`
)

, holdings_all as (
select      a.account_number
            ,a.portfolio_id
            ,e.account_sub_type
            ,a.period_start
            ,evt.customer_affluence
            ,evt.customer_income_band
            ,evt.customer_age_band
            ,evt.comms_bb_tenure_latest
            ,evt.tv_tenure_latest
            ,evt.customer_product_holding_type
            ,evt.comms_bb_current_hub_type
            ,a.event_dt
            ,b.subscription_id
            ,b.subscription_created_dt
            ,b.history_order
            ,b.status
            ,b.status_code
            ,b.catalogue_product_name as bundle
            ,b.entitlement
            ,b.sub_type
            ,f.bb_speed_category
            ,f.bb_tech_category
            ,f.speed as bb_speed
            ,ifnull(case when upper(b.status_code) in ('AC', 'AB', 'PC', 'PT', 'CF', 'BCRQ','RI', 'PR', 'PC', 'FBP', 'FBI', 'A', 'R', 'CRQ')  then f.rate else 0 end,0) as or_rental
            -- breakdown
            -- Updated the defination for BB, Boost, Talk and LR
            ,case       when b.status_code in ('AC', 'AB', 'PC', 'PT', 'CF', 'BCRQ')and b.sub_type = 'Broadband DSL Line' then 'BB'
                        when b.status_code in ('AC', 'AB', 'PC', 'PT', 'CF', 'BCRQ') and (lower(b.sub_type) like '%boost%' or upper(b.sub_type) = 'WIFI_RELATED_SUBS') then 'Boost'                           
                        when b.status_code in ('RI', 'PC', 'FBP', 'FBI', 'BCRQ', 'A') and (b.entitlement in (
                         'Sky Pay As You Talk'      
                        ,'Sky Pay As You Talk (VoIP)'                          
                        ,'Sky Talk Evenings and Weekends Extra (Weekends)'
                        ,'Sky Talk Evenings and Weekends Extra (Freetime)'
                        ,'Sky Talk Evenings and Weekends Extra (VoIP)'
                        ,'Sky Talk Evenings and Weekends Extra'
                        ,'Sky Talk Anytime Extra (Anytime)'
                        ,'Sky Talk Anytime Extra'
                        ,'Sky Talk Anytime Extra (Anytime UK)'
                        ,'Sky Talk Anytime Extra (24/7)'
                        ,'Sky Talk Anytime Extra (VoIP)'
                        ,'Sky Talk Anytime (VoIP)'
                        ,'Sky Talk Extra'
                        ,'Sky Talk Freetime'
                        ,'Sky Talk Freetime (VoIP)'
                        ,'Sky Talk Unlimited'
                        ,'Sky Talk International Extra'
                        ,'Sky Talk International Extra (Anytime International)'
                        ,'Sky Talk International Extra (VoIP)'
                        ,'Sky Talk International Extra (24/7 International)'
                        ,'Sky Talk International Extra (Super Unlimited)'
                        ,'Sky Talk Super Unlimited'
                        ,'Sky Talk 24 / 7'
                        ,'Sky Talk 24 / 7 + International'
                     )
                )  then 'Talk'
                        when b.status_code in ('R', 'CRQ', 'BCRQ', 'A') and (replace(lower(b.sub_type), ' ', '_') like '%line_rental%')  then 'LR'
                        end as product
            -- contracts
            ,cast(c.created_dt as date) as contract_created_dt
            ,case       when upper(b.status_code) in ('AC', 'AB', 'PC', 'PT', 'CF', 'BCRQ','RI', 'PR', 'PC', 'FBP', 'FBI', 'A', 'R', 'CRQ') 
                        then cast(c.start_dt as date) else null end as contract_start_date
            ,case       when upper(b.status_code) in ('AC', 'AB', 'PC', 'PT', 'CF', 'BCRQ','RI', 'PR', 'PC', 'FBP', 'FBI', 'A', 'R', 'CRQ') 
                        then case   when cast(c.end_dt as date) is null then cast(c.calculated_minimum_term_end_dt as date)
                                    else cast(c.end_dt as date) end
                        else null end as contract_end_date
            ,case       when c.id is not null 
                        and upper(b.status_code) in ('AC', 'AB', 'PC', 'PT', 'CF', 'BCRQ','RI', 'PR', 'PC', 'FBP', 'FBI', 'A', 'R', 'CRQ') 
                        then 1 else 0 end as in_contract_flag   
            -- flags
            ,case when upper(b.status_code) in ('AC', 'AB', 'PC', 'PT', 'CF', 'BCRQ','RI', 'PR', 'PC', 'FBP', 'FBI', 'A', 'R', 'CRQ') then 1 else 0 end as active_flag
            ,case when upper(b.status_code) in ('PO', 'SC', 'TERMINATED', 'CEASED','CN') then 1 else 0 end as churned_flag
            -- rack rates
            ,case when b.status_code in ('R', 'CRQ', 'BCRQ', 'A') and (replace(lower(b.sub_type), ' ', '_') like '%line_rental%') then coalesce(b.catalogue_product_price,0) --lr
                  when b.status_code in ('RI', 'PR', 'PC', 'FBP', 'FBI', 'BCRQ', 'A')
                     and (b.entitlement in (
                         'Sky Pay As You Talk'      
                        ,'Sky Pay As You Talk (VoIP)'                          
                        ,'Sky Talk Evenings and Weekends Extra (Weekends)'
                        ,'Sky Talk Evenings and Weekends Extra (Freetime)'
                        ,'Sky Talk Evenings and Weekends Extra (VoIP)'
                        ,'Sky Talk Evenings and Weekends Extra'
                        ,'Sky Talk Anytime Extra (Anytime)'
                        ,'Sky Talk Anytime Extra'
                        ,'Sky Talk Anytime Extra (Anytime UK)'
                        ,'Sky Talk Anytime Extra (24/7)'
                        ,'Sky Talk Anytime Extra (VoIP)'
                        ,'Sky Talk Anytime (VoIP)'
                        ,'Sky Talk Extra'
                        ,'Sky Talk Freetime'
                        ,'Sky Talk Freetime (VoIP)'
                        ,'Sky Talk Unlimited'
                        ,'Sky Talk International Extra'
                        ,'Sky Talk International Extra (Anytime International)'
                        ,'Sky Talk International Extra (VoIP)'
                        ,'Sky Talk International Extra (24/7 International)'
                        ,'Sky Talk International Extra (Super Unlimited)'
                        ,'Sky Talk Super Unlimited'
                        ,'Sky Talk 24 / 7'
                        ,'Sky Talk 24 / 7 + International'
                     )) then coalesce(b.catalogue_product_price,0) --talk
                  when upper(b.status_code) in ('AC', 'AB', 'PC', 'PT', 'CF', 'BCRQ') and b.sub_type = 'Broadband DSL Line'  or (lower(b.sub_type) like '%boost%'or upper(b.sub_type) = 'WIFI_RELATED_SUBS') then coalesce(b.catalogue_product_price,0) --bb_boost
                  else 0 end as rack_rate          
            -- offers
            -- tvfbbo 
            -- It will only be for BB, which means it will always be 0 for boost, talk and lr
            ,sum( CASE WHEN (
                lower(offer_bill_name) IN ('broadband subscription discounted', 'broadband subscription discount', 'tv discount with 6 months broadband unlimited offer')
                AND (((SELECT COUNT(*) FROM `skyuk-uk-per-sub-economic-poc.rct02_uploads.tv_funded_bb_offers_upload` g WHERE g.description = offer_description) > 0 
                  OR (lower(offer_description) LIKE '%tv%' OR lower(offer_description) LIKE '%stream%' OR lower(offer_description) LIKE '%glass%' OR lower(offer_description) LIKE '%signature%' OR lower(offer_description) LIKE '%ultimate%'))
                AND lower(offer_description) NOT LIKE '%now%' AND lower(offer_description) NOT LIKE '%no tv%' AND lower(offer_description) NOT LIKE '%roi%'AND lower(offer_description) NOT LIKE '%not%')) THEN CASE WHEN g.offer_id IS NOT NULL THEN round(coalesce(-discount_value,0), 2) ELSE 0 END ELSE 0 END) as tvfbbo
            -- ICD
            -- It will only be for BB, which means it will always be 0 for boost, talk and lr
            ,sum(-- bb
                  case when upper(b.status_code) in ('AC', 'AB', 'PC', 'PT', 'CF', 'BCRQ') and b.sub_type = 'Broadband DSL Line'  
                        and d.in_contract_offer_flag = true 
                        and (lower(offer_description) like '%fibre%' or lower(offer_description) like '%broadband%'or lower(offer_description) like '%connect%'or lower(offer_description) like '%superfast%'or lower(offer_description) like '%ultrafast%' or lower(offer_description) like '%ftth%'or lower(offer_description) like '%essential%')
                        and (lower(offer_description) not like '%super fibre%'and lower(offer_description) not like '%brilliant broadband%' and lower(offer_description) not like '%fab fibre%')
                        and lower(offer_description) not like '%boost%'and lower(offer_description) not like '%sky broadband buddy%'
                        and (lower(offer_bill_name) like '%contract%discount%' or lower(offer_bill_name) like '%broadband%')
                        and coalesce(d.price_freeze_offer_flag, False) <> True then round(-coalesce(discount_value,0), 2)                  
                        else 0 end)   as in_contract_discount
            -- software_offer
            ,sum(case   when (d.in_contract_offer_flag != True or d.in_contract_offer_flag is null) 
                        and g.offer_id is null
                        and upper(b.status_code) in ('AC', 'AB', 'PC', 'PT', 'CF', 'BCRQ','RI', 'PR', 'PC', 'FBP', 'FBI', 'A', 'R', 'CRQ') 
                        then round(-coalesce(discount_value,0), 2) else 0 end) as software_offer
from        accounts_and_dates a
left join   bb_talk b
on          a.portfolio_id = b.portfolio_id
and         a.account_number = b.account_number
and         a.event_dt < cast(b.effective_to_dt as date)
and         a.event_dt >= cast(b.effective_from_dt as date)
and         date(b.effective_to_dt) <> date(b.effective_from_dt)
left join   `skyuk-uk-customer-pres-prod.uk_pub_cust_spine_subs_is.dim_subscription_agreement_item` c
on          b.subscription_id = c.subscription_id
and         minimum_term_months > 0
and         cast(c.start_dt as date) <= a.event_dt
and         case  when cast(c.end_dt as date) is null then cast(c.calculated_minimum_term_end_dt as date)
                  else cast(c.end_dt as date) end > event_dt
left join   `skyuk-uk-customer-pres-prod.uk_pub_customer_spine_offer_is.mart_offer` d
on          a.account_number = d.account_number
and         a.portfolio_id = d.portfolio_id
and         b.subscription_id = d.subscription_id
and         a.event_dt >= cast(d.effective_from_dt as date)
and         a.event_dt < cast(d.effective_to_dt as date) 
and         d.discount_type != 'PRICE PROTECTION'
and         d.active_flag
left join   `skyuk-uk-per-sub-economic-poc.spine_replicas.mart_account` e
on          b.portfolio_id = e.portfolio_id
and         b.account_number = e.account_number
and         a.event_dt >= date(e.effective_from_dt)
and         a.event_dt < date(e.effective_to_dt) 
left join   `skyuk-uk-per-sub-economic-poc.BS.Product_Mapping_with_rates_Mar23` f
on          b.entitlement = f.entitlement
and         last_day(a.event_dt,month) = f.effective_month
left join   (select distinct offer_id from `skyuk-uk-per-sub-economic-poc.rct02_uploads.tv_funded_bb_offers_upload`) g
on          d.offer_detail_id = cast(g.offer_id as string)
left join   `skyuk-uk-per-sub-economic-poc.Per_Sub_Economics_Finance.New_Everything_Comcast_Monthly` evt
on          a.account_number = evt.account_number
and         a.event_dt = evt.event_dt
-- Added from EVT
where b.catalogue_product_type = 'RC'
and     (d.active_flag)
and      d.offer_transaction_type = 'RC'

group by    a.account_number
            ,a.portfolio_id
            ,e.account_sub_type
            ,customer_affluence
            ,customer_income_band
            ,customer_age_band
            ,comms_bb_tenure_latest
            ,tv_tenure_latest
            ,customer_product_holding_type
            ,comms_bb_current_hub_type
            ,a.event_dt
            ,a.period_start
            ,b.subscription_id
            ,b.subscription_created_dt
            ,b.history_order
            ,b.status
            ,b.status_code
            ,bundle
            ,b.entitlement
            ,b.sub_type
            ,f.bb_speed_category
            ,f.bb_tech_category
            ,bb_speed
            ,f.rate      
            ,product
            ,contract_created_dt
            ,contract_start_date
            ,contract_end_date
            ,in_contract_flag
            ,active_flag            
            ,rack_rate
qualify     rank () over     (partition by account_number
                                          ,event_dt
                                          ,product
                              order by    case when product = 'Talk' and upper(status_code) in ('AC', 'AB', 'PC', 'PT', 'CF', 'BCRQ','RI', 'PR', 'PC', 'FBP', 'FBI', 'A', 'R', 'CRQ')  then 0 else 1 end
                                          ,subscription_created_dt desc
                                          ,history_order desc) = 1 
and         rank () over     (partition by account_number
                                          ,event_dt
                                          ,subscription_id 
                              order by    contract_created_dt desc
                                          ,contract_end_date desc
                                          ,contract_start_date) = 1             
)

select      *
            ,case when contract_start_date between period_start and event_dt then 1 else 0 end as contract_added
            ,lag(rack_rate) over (partition by account_number, product order by event_dt) as prev_rack
            ,lag(bundle) over (partition by account_number, product order by event_dt) as prev_bundle
            ,lag(entitlement) over (partition by account_number, product order by event_dt) as prev_entitlement
from        holdings_all
where       product is not null
and         case when product = 'Talk' and rack_rate = 0 and upper(status_code) in ('AC', 'AB', 'PC', 'PT', 'CF', 'BCRQ','RI', 'PR', 'PC', 'FBP', 'FBI', 'A', 'R', 'CRQ') 
            then 0 else 1 end = 1
;

create or replace table `skyuk-uk-per-sub-economic-poc.Revenue_Change_Analysis.BB_Rev_Change_Monthly_Account`
as

with bb_only_agg as (
select      account_number
            ,portfolio_id
            ,event_dt
            ,contract_added as bb_contract_added
            ,in_contract_flag as bb_in_contract_flag
            ,active_flag as bb_active_flag
            ,churned_flag as bb_churned_flag
            ,contract_end_date as bb_contract_end_date
            ,bb_speed_category
            ,bb_tech_category
            ,bb_speed
            -- ,mobile_any_flag
            ,customer_affluence
            ,customer_income_band
            ,customer_age_band
            ,comms_bb_tenure_latest
            ,tv_tenure_latest
            ,customer_product_holding_type
            ,comms_bb_current_hub_type
from        `skyuk-uk-per-sub-economic-poc.Revenue_Change_Analysis.BB_Base_Monthly`
where       product = 'BB'             
)

,bridging as (
select      distinct
            account_number
            ,last_day(eow,month) as event_dt
from        `skyuk-uk-per-sub-economic-poc.dtv_bridging.FM_Dataset_5`
where       cohort_2 in (1,3)
and         weeks_rel_eoo = 0
)

,bb_holdings_agg as (
select      account_number
            ,portfolio_id
            ,account_sub_type
            ,event_dt
            ,case when product in ('BB','LR') then 'BB' else product end as product          
            -- contracts
            ,max(case when product = 'LR' then null else contract_start_date end) as contract_start_date
            ,max(case when product = 'LR' then null else contract_end_date end) as contract_end_date
            ,max(case when product = 'LR' then null else in_contract_flag end) as in_contract_flag
            ,max(case when product = 'LR' then null else contract_added end) as contract_added
            -- flags
            ,max(case when product = 'LR' then null else active_flag end) as active_flag
            -- rack rates
            ,sum(rack_rate) as rack_rate
            ,max(case when rack_rate > prev_rack
                      and entitlement = prev_entitlement
                      and product != 'LR'
                      then 1 else 0 end) as PINC_flag
            --offers
            ,sum(in_contract_discount) as in_contract_discount
            ,sum(tvfbbo) as tvfbbo
            ,sum(software_offer) as software_offer
            ,sum(or_rental) as or_rental
            --net rev
            ,sum(rack_rate + in_contract_discount + software_offer + tvfbbo) as net_revenue
from        `skyuk-uk-per-sub-economic-poc.Revenue_Change_Analysis.BB_Base_Monthly`          
group by    account_number
            ,portfolio_id
            ,account_sub_type
            ,event_dt
            ,product         
)

,lookback_prod as    (
select      a.account_number
            ,a.portfolio_id
            ,a.event_dt
            ,a.product
            ,lag(a.in_contract_flag)      over (partition by a.account_number, a.product order by a.event_dt) as prev_in_contract_flag
            ,lag(a.active_flag)           over (partition by a.account_number, a.product order by a.event_dt) as prev_active_flag
            ,lag(a.rack_rate)             over (partition by a.account_number, a.product order by a.event_dt) as prev_rack_rate
            ,lag(a.in_contract_discount)  over (partition by a.account_number, a.product order by a.event_dt) as prev_in_contract_discount
            ,lag(a.tvfbbo)                over (partition by a.account_number, a.product order by a.event_dt) as prev_tvfbbo
            ,lag(a.software_offer)        over (partition by a.account_number, a.product order by a.event_dt) as prev_software_offer
            ,lag(a.or_rental)             over (partition by a.account_number, a.product order by a.event_dt) as prev_or_rental
            ,lag(a.net_revenue)           over (partition by a.account_number, a.product order by a.event_dt) as prev_net_revenue
            ,lag(a.contract_end_date)     over (partition by a.account_number, a.product order by a.event_dt) as prev_contract_end_date
from        bb_holdings_agg a
)

,lookback_acc as    (
select      a.account_number
            ,a.portfolio_id
            ,a.event_dt
            ,lag(a.bb_in_contract_flag)   over (partition by a.account_number order by a.event_dt) as prev_bb_in_contract_flag
            ,lag(a.bb_active_flag)        over (partition by a.account_number order by a.event_dt) as prev_bb_active_flag
            ,lag(a.bb_churned_flag)       over (partition by a.account_number order by a.event_dt) as prev_bb_churned_flag
            ,lag(a.bb_contract_end_date)  over (partition by a.account_number order by a.event_dt) as prev_bb_contract_end_date
            ,lag(a.bb_speed_category)     over (partition by a.account_number order by a.event_dt) as prev_bb_speed_category
            ,lag(a.bb_tech_category)      over (partition by a.account_number order by a.event_dt) as prev_bb_tech_category
            ,lag(a.bb_speed)              over (partition by a.account_number order by a.event_dt) as prev_bb_speed
            -- ,lag(a.mobile_any_flag)          over (partition by a.account_number order by a.event_dt) as prev_mobile_any_flag
            ,lag(a.customer_affluence)    over (partition by a.account_number order by a.event_dt) as prev_customer_affluence
            ,lag(a.customer_income_band)  over (partition by a.account_number order by a.event_dt) as prev_customer_income_band
            ,lag(a.customer_age_band)     over (partition by a.account_number order by a.event_dt) as prev_customer_age_band
            ,lag(a.comms_bb_tenure_latest)over (partition by a.account_number order by a.event_dt) as prev_comms_bb_tenure_latest
            ,lag(a.tv_tenure_latest)      over (partition by a.account_number order by a.event_dt) as prev_tv_tenure_latest
            ,lag(a.customer_product_holding_type) over (partition by a.account_number order by a.event_dt) as prev_customer_product_holding_type
            ,lag(a.comms_bb_current_hub_type) over (partition by a.account_number order by a.event_dt) as prev_comms_bb_current_hub_type
from        bb_only_agg a
)

,initial_output as
(
select      a.*
            ,ifnull(b.bb_in_contract_flag,0) as bb_in_contract_flag
            ,ifnull(b.bb_contract_added,0) as bb_contract_added
            ,ifnull(b.bb_active_flag,0) as bb_active_flag
            ,ifnull(b.bb_churned_flag,0) as bb_churned_flag         
            ,ifnull(d.prev_in_contract_flag,0) as prev_in_contract_flag
            ,ifnull(d.prev_active_flag,0) as prev_active_flag
            ,ifnull(d.prev_rack_rate,0) as prev_rack_rate
            ,ifnull(d.prev_in_contract_discount,0) as prev_in_contract_discount
            ,ifnull(d.prev_tvfbbo,0) as prev_tvfbbo
            ,ifnull(d.prev_software_offer,0) as prev_software_offer
            ,ifnull(d.prev_or_rental,0) as prev_or_rental
            ,ifnull(d.prev_net_revenue,0) as prev_net_revenue
            ,ifnull(e.prev_bb_in_contract_flag,0) as prev_bb_in_contract_flag
            ,ifnull(e.prev_bb_active_flag,0) as prev_bb_active_flag
            ,ifnull(e.prev_bb_churned_flag,0) as prev_bb_churned_flag
            -- ,ifnull(b.mobile_any_flag
            ,ifnull(b.customer_affluence,e.prev_customer_affluence) as customer_affluence
            ,ifnull(b.customer_income_band,e.prev_customer_income_band) as customer_income_band
            ,ifnull(b.customer_age_band,e.prev_customer_age_band) as customer_age_band
            ,ifnull(b.comms_bb_tenure_latest,e.prev_comms_bb_tenure_latest) as comms_bb_tenure_latest
            ,ifnull(b.tv_tenure_latest,e.prev_tv_tenure_latest) as tv_tenure_latest
            ,ifnull(b.customer_product_holding_type,e.prev_customer_product_holding_type) as customer_product_holding_type
            ,ifnull(b.comms_bb_current_hub_type,e.prev_comms_bb_current_hub_type) as comms_bb_current_hub_type
            ,bb_speed_category
            ,bb_tech_category
            ,bb_speed
            ,prev_bb_speed_category
            ,prev_bb_tech_category
            ,prev_bb_speed
            ,ifnull(net_revenue,0) - ifnull(prev_net_revenue,0) as nr_delta
            ,case when ifnull(prev_bb_active_flag,0) = 0 and ifnull(active_flag,0) = 1                      then 'New Customer'
                  when ifnull(prev_bb_active_flag,0) = 1 and ifnull(bb_active_flag,0) = 0
                  and ifnull(active_flag,0) = 1 and a.product = 'Talk'                                      then 'Churn BB Keep Talk'    
                  when ifnull(prev_bb_active_flag,0) = 1 and ifnull(bb_active_flag,0) = 0                   then 'Churned Customer'        
                  when bb_speed > prev_bb_speed 
                  and ifnull(b.bb_contract_added,0) = 1 
                  and bb_tech_category = 'FTTP'                                                             then 'FTTP Upgrade Customer'
                  when bb_speed > prev_bb_speed 
                  and ifnull(b.bb_contract_added,0) = 1 
                  and bb_tech_category in ('FTTC','SOGEA')                                                  then 'FTTC Upgrade Customer'
                  when bb_speed < prev_bb_speed 
                  and ifnull(b.bb_contract_added,0) = 1 
                  and bb_tech_category = 'FTTP'                                                             then 'FTTP Downgrade Customer'
                  when bb_speed < prev_bb_speed 
                  and ifnull(b.bb_contract_added,0) = 1 
                  and bb_tech_category in ('FTTC','SOGEA')                                                  then 'FTTC Downgrade Customer'
                  when prev_bb_tech_category != bb_tech_category 
                  and ifnull(b.bb_contract_added,0) = 1                                                     then 'Regrade'          
                  when bridging.account_number is not null                                                  then 'Bridged Customer'
                  when bb_speed = prev_bb_speed
                  and ifnull(b.bb_contract_added,0) = 1                                                     then 'Recontracted BB Customer'
                  when ifnull(d.prev_in_contract_flag,0) = 1 and ifnull(a.in_contract_flag,0) = 0           then 'OOC Customer'
                  when ifnull(d.prev_software_offer,0) < 0 and a.software_offer = 0                         then 'Offer Roll-off'      
                  when a.product = 'BB' 
                  and ifnull(d.prev_software_offer,0) = -ifnull(d.prev_rack_rate,0) 
                  and ifnull(d.prev_software_offer,0) < a.software_offer                                    then 'Free BB Roll-off'
                  when a.product = 'Talk' 
                  and rack_rate > ifnull(d.prev_rack_rate,0)                                                then 'Talk Spin Up'  
                  when a.product = 'Talk' 
                  and a.software_offer > ifnull(d.prev_software_offer,0) 
                  and a.software_offer < 0                                                                  then 'Call Offer Reduction'
                  when ifnull(prev_active_flag,0) = 1 and ifnull(active_flag,0) = 0 
                  and ifnull(bb_active_flag,0) = 1                                                          then 'Spin Down'  
                  when prev_bb_tech_category != bb_tech_category 
                  and ifnull(b.bb_contract_added,0) = 0                                                     then 'Regrade without contract' 
                  when prev_bb_speed_category != bb_speed_category 
                  and ifnull(b.bb_contract_added,0) = 0                                                     then 'Speed change without contract'
                  when ifnull(d.prev_software_offer,0) > a.software_offer                                   then 'Offer Starting' 
                  when ifnull(d.prev_software_offer,0) < a.software_offer                                   then 'Offer Ending'  
                  end                                                                                       as bb_movement
from        bb_holdings_agg a
left join   bb_only_agg b
on          a.account_number = b.account_number
and         a.portfolio_id = b.portfolio_id
and         a.event_dt = b.event_dt
left join   lookback_prod d
on          a.account_number = d.account_number
and         a.portfolio_id = d.portfolio_id
and         a.event_dt = d.event_dt
and         a.product = d.product
left join   lookback_acc e
on          a.account_number = e.account_number
and         a.portfolio_id = e.portfolio_id
and         a.event_dt = e.event_dt
left join   bridging
on          a.account_number = bridging.account_number
and         a.event_dt = bridging.event_dt
)

select      *
            ,lag(net_revenue,12) over (partition by account_number order by event_dt) as prev_year_nr
            ,lag(tvfbbo,12) over (partition by account_number order by event_dt) as prev_year_tvfbbo
            ,lag(or_rental,12) over (partition by account_number order by event_dt) as prev_year_or_rental
            ,lag(bb_movement,12) over (partition by account_number order by event_dt) as prev_year_bb_movement
from        initial_output 
where       (bb_active_flag = 1 or prev_bb_active_flag = 1)
and         event_dt >= '2019-01-01'          
;