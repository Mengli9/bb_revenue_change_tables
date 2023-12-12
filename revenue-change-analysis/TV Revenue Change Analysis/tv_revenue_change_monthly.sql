create temp table tv_base_monthly
as

with dt_array as (
select      *
from unnest(generate_date_array('2019-09-01',current_date(), interval 1 month)) as period_start
)

,dates as (
select      last_day(period_start,month) as event_dt
from        dt_array
where       last_day(period_start,month) <= current_date()        
)

,accounts as (
select      distinct
            account_number
            ,portfolio_id
from        dates dt
inner join  `skyuk-uk-customer-pres-prod.uk_pub_cust_spine_subs_is.mart_subscription_dtv` msd
on          dt.event_dt < cast(msd.effective_to_dt as date)
and         dt.event_dt >= cast(msd.effective_from_dt as date)
and         date(msd.effective_to_dt) <> date(msd.effective_from_dt)
where       msd.status_code in ('AC', 'AB', 'PC', 'ACTIVE', 'PENDING_CEASE', 'BLOCKED')
and 	      msd.sub_type in ('DTV Primary Viewing', 'SOIP BASEPACK')
and         currency_code = 'GBP'

)

select      dates.*
            ,accounts.*
from        accounts
cross join  dates
;

create or replace table `skyuk-uk-per-sub-economic-poc.Revenue_Change_Analysis.TV_Base_Monthly`
as

with holdings_all as (
select      a.account_number
            ,a.portfolio_id
            ,b.account_type
            ,e.account_sub_type
            ,a.event_dt
            ,b.subscription_id
            ,b.subscription_created_dt
            ,b.effective_from_dt
            ,b.effective_to_dt
            ,b.history_order
            ,b.status
            ,b.status_code
            ,case when b.status_code in ('AC', 'AB', 'PC', 'ACTIVE', 'PENDING_CEASE', 'BLOCKED') and b.sub_type in ('DTV Primary Viewing', 'SOIP BASEPACK') then b.catalogue_product_name else null end as bundle
            ,b.entitlement
            ,b.sub_type
            ,g.tv_latest_order_rtm_1
            ,g.tv_latest_acquisition_rtm_1
            ,g.ta_calls
            ,g.platform_calls
            ,g.service_calls
            ,g.upgrade_calls
            ,g.Value_calls
            ,g.billing_calls
            ,g.comms_bbcoe_calls
            ,g.dort_calls
            ,g.rtm_calls
            ,g.winback_calls
            ,g.total_calls
            ,g.ta_web
            -- breakdown
            ,case       when lower(b.sub_type) in ('dtv primary viewing', 'soip basepack')                        then 'Base TV'
                        when b.sub_type in ('DTV HD')                                                             then 'DTV HD'
                        when b.sub_type in ( 'HD Pack')                                                           then 'HD Pack'  
                        when lower(b.catalogue_product_name) like '%multiscreen%' 
                        or lower(b.catalogue_product_name) like '%whole home%'                                    then 'Whole Home/MS' 
                        when lower(b.sub_type) like '%box_sets%'                                                  then 'Boxsets'
                        when lower(b.catalogue_product_name) in ('disney+','disney+ (legacy)')                    then 'Disney+'                     
                        when lower(b.sub_type) in ('bt sport','btsport')  
                        or lower(b.entitlement) like '%tnt sport%' 
                        or lower(b.catalogue_product_name) like '%tnt sport%'                                     then 'BT Sports'
                        when lower(b.sub_type) like '%kids%'                                                      then 'Kids'
                        when lower(b.catalogue_product_name) 
                              in ('netflix basic','netflix standard','netflix premium' )                          then 'Netflix'
                        when lower(b.catalogue_product_name) 
                              in ('ultimate tv add on' )                                                          then 'Ultimate TV Add On'
                        when lower(b.catalogue_product_name) like '%sky go%'                                      then 'SGE'
                        when lower(b.catalogue_product_name) like '%ultimate on demand%'                          then 'UoD'
                        when lower(b.sub_type) = 'cinema'                                                         then 'Cinema'
                        when lower(b.sub_type) = 'sports'                                                         then 'Sports' 
                        when lower(b.sub_type) like '%uhd%'                                                       then 'UHD'
                        when lower(b.sub_type) = 'SOIP SKIP ADVERTS' and lower(b.entitlement) = 'Ad skipping'     then 'Ad Skip'
                        end as sub_product
            -- breakdown
            ,case       when lower(b.sub_type) in ('dtv primary viewing', 'soip basepack')                        then 'Base TV'
                        when b.sub_type in ('DTV HD', 'HD Pack')                                                  then 'HD'                           
                        when lower(b.catalogue_product_name) like '%multiscreen%' 
                        or lower(b.catalogue_product_name) like '%whole home%'                                    then 'Whole Home/MS' 
                        when lower(b.sub_type) like '%box_sets%'                                                  then 'Boxsets'
                        when lower(b.catalogue_product_name) in ('disney+','disney+ (legacy)')                    then 'Disney+'                     
                        when lower(b.sub_type) in ('bt sport','btsport')  
                        or lower(b.entitlement) like '%tnt sport%' 
                        or lower(b.catalogue_product_name) like '%tnt sport%'                                       then 'BT Sports'
                        when lower(b.sub_type) like '%kids%'                                                      then 'Kids'
                        when lower(b.catalogue_product_name) 
                              in ('netflix basic','netflix standard','netflix premium','ultimate tv add on' )     then 'Netflix'
                        when lower(b.catalogue_product_name) like '%sky go%'                                      then 'SGE'
                        when lower(b.catalogue_product_name) like '%ultimate on demand%'                          then 'UoD'
                        when lower(b.sub_type) = 'cinema'                                                         then 'Cinema'
                        when lower(b.sub_type) = 'sports'                                                         then 'Sports' 
                        when lower(b.sub_type) like '%uhd%'                                                       then 'UHD'
                        when lower(b.sub_type) = 'SOIP SKIP ADVERTS' and lower(b.entitlement) = 'Ad skipping'     then 'Ad Skip'
                        end as product
            -- contracts
            ,c.created_dt as contract_created_dt
            ,case       when upper(b.status_code) in ('AC', 'AB', 'PC', 'ACTIVE', 'PENDING_CEASE', 'BLOCKED')
                        then cast(c.start_dt as date) else null end as contract_start_date
            ,case       when upper(b.status_code) in ('AC', 'AB', 'PC', 'ACTIVE', 'PENDING_CEASE', 'BLOCKED')
                        then case   when cast(c.end_dt as date) is null then cast(c.calculated_minimum_term_end_dt as date)
                                    else cast(c.end_dt as date) end
                        else null end as contract_end_date
            ,case       when c.id is not null 
                        and upper(b.status_code) in ('AC', 'AB', 'PC', 'ACTIVE', 'PENDING_CEASE', 'BLOCKED')
                        then 1 else 0 end as in_contract_flag   
            -- flags
            ,case when upper(b.status_code) in ('AC', 'AB', 'PC', 'ACTIVE', 'PENDING_CEASE', 'BLOCKED') then 1 else 0 end as active_flag
            ,case when upper(b.status_code) in ('PO', 'SC', 'TERMINATED', 'CEASED') then 1 else 0 end as churned_flag
            -- rack rates
            ,sum(case when upper(b.status_code) in ('AC', 'AB', 'PC', 'ACTIVE', 'PENDING_CEASE', 'BLOCKED') then coalesce(b.catalogue_product_price,0) else 0 end) as rack_rate          
            --offers
            ,sum(case   when in_contract_offer_flag = True 
                        and upper(b.status_code) in ('AC', 'AB', 'PC', 'ACTIVE', 'PENDING_CEASE', 'BLOCKED')
                        then round(-coalesce(discount_value,0), 2) else 0 end) as in_contract_discount
            ,sum(case   when (in_contract_offer_flag != True or in_contract_offer_flag is null)
                        and upper(b.status_code) in ('AC', 'AB', 'PC', 'ACTIVE', 'PENDING_CEASE', 'BLOCKED')
                        then round(-coalesce(discount_value,0), 2) else 0 end) as software_offer
from        tv_base_monthly a
left join   `skyuk-uk-customer-pres-prod.uk_pub_cust_spine_subs_is.mart_subscription_dtv` b
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
left join   `skyuk-uk-per-sub-economic-poc.Per_Sub_Economics_Finance.Everything_Comcast_Monthly` g 
on          a.event_dt = g.event_dt
and         a.account_number = g.account_number  
where       b.status_code in ('AC', 'AB', 'PC', 'ACTIVE', 'PENDING_CEASE', 'BLOCKED')
and 	    b.sub_type in ('DTV Primary Viewing', 'SOIP BASEPACK')
and         b.currency_code = 'GBP'
group by    a.account_number
            ,a.portfolio_id
            ,b.account_type
            ,e.account_sub_type
            ,a.event_dt
            ,b.subscription_id
            ,b.subscription_created_dt
            ,b.effective_from_dt
            ,b.effective_to_dt
            ,b.history_order
            ,b.status
            ,b.status_code
            ,bundle
            ,b.entitlement
            ,b.sub_type
            ,g.tv_latest_order_rtm_1
            ,g.tv_latest_acquisition_rtm_1
            ,g.ta_calls
            ,g.platform_calls
            ,g.service_calls
            ,g.upgrade_calls
            ,g.Value_calls
            ,g.billing_calls
            ,g.comms_bbcoe_calls
            ,g.dort_calls
            ,g.rtm_calls
            ,g.winback_calls
            ,g.total_calls
            ,g.ta_web
            ,sub_product     
            ,product
            ,contract_created_dt
            ,contract_start_date
            ,contract_end_date
            ,in_contract_flag
            ,active_flag            
qualify     row_number () over      (partition by account_number
                                                ,event_dt
                                                ,product
                                                ,sub_product
                                    order by    subscription_created_dt desc
                                                ,history_order desc
                                                ,b.effective_from_dt desc
                                                ,effective_to_dt desc) = 1 
and         row_number () over      (partition by account_number
                                                ,event_dt
                                                ,subscription_id 
                                    order by    contract_created_dt desc
                                                ,contract_end_date desc
                                                ,contract_start_date) = 1             
)

select      *
            ,lag(rack_rate) over (partition by account_number, product order by event_dt) as prev_rack
            ,lag(bundle) over (partition by account_number, product order by event_dt) prev_bundle
from        holdings_all 
where       product is not null
;

create or replace table `skyuk-uk-per-sub-economic-poc.Revenue_Change_Analysis.TV_Rev_Change_Monthly_Account`
as

with base_only_agg as (
select      account_number
            ,portfolio_id
            ,account_type
            ,event_dt
            ,case       when upper(account_type) = 'SOIP'   then 'SOIP'
                        when bundle like '%Entertainment%'  then 'Ents'
                        when bundle like '%TV Essentials%'  then 'Essentials'
                        when bundle like '%Signature%'      then 'Sky Signature'
                        when bundle like '%Basics%'         then 'Sky Basics'
                        when bundle is null                 then null
                        else 'Legacy' end as tv_bundle_category
            ,in_contract_flag as tv_in_contract_flag
            ,active_flag as tv_active_flag
            ,churned_flag as tv_churned_flag
            ,contract_start_date as tv_contract_start_date
            ,contract_end_date as tv_contract_end_date
            ,case when event_dt between contract_start_date and contract_end_date then 1 else 0 end as tv_in_contract_flag_v2
from        `skyuk-uk-per-sub-economic-poc.Revenue_Change_Analysis.TV_Base_Monthly`
where       product = 'Base TV'             
)

,tv_port_agg as (
select      portfolio_id
            ,event_dt
            ,max(case when active_flag = 1 and upper(a.account_type) = 'SOIP' then 1 else 0 end) as tv_soip_flag
            ,max(a.active_flag) as tv_any_active_flag
            ,max(tv_bundle_category) as tv_bundle_category_portfolio
from        `skyuk-uk-per-sub-economic-poc.Revenue_Change_Analysis.TV_Base_Monthly` a
left join base_only_agg using(event_dt, portfolio_id)
where       product = 'Base TV'
group by    portfolio_id
            ,event_dt                      
)

,tv_holdings_agg as (
select      account_number
            ,portfolio_id
            ,account_type
            ,account_sub_type
            ,event_dt
            ,product
            ,tv_latest_order_rtm_1
            ,tv_latest_acquisition_rtm_1
            ,ta_calls
            ,platform_calls
            ,service_calls
            ,upgrade_calls
            ,Value_calls
            ,billing_calls
            ,comms_bbcoe_calls
            ,dort_calls
            ,rtm_calls
            ,winback_calls
            ,total_calls
            ,ta_web
            -- contracts
            ,max(contract_start_date) as contract_start_date
            ,max(contract_end_date) as contract_end_date
            ,max(in_contract_flag) as in_contract_flag 
            -- flags
            ,max(active_flag) as active_flag
            -- rack rates
            ,sum(rack_rate) as rack_rate
            ,max(case when rack_rate > prev_rack
                      and bundle = prev_bundle
                      then 1 else 0 end) as PINC_flag
            --offers
            ,sum(in_contract_discount) as in_contract_discount
            ,sum(software_offer) as software_offer
            --net rev
            ,sum(rack_rate + in_contract_discount + software_offer) as net_revenue
from        `skyuk-uk-per-sub-economic-poc.Revenue_Change_Analysis.TV_Base_Monthly`          
group by    account_number
            ,portfolio_id
            ,account_type
            ,account_sub_type
            ,event_dt
            ,product
            ,tv_latest_order_rtm_1
            ,tv_latest_acquisition_rtm_1
            ,ta_calls
            ,platform_calls
            ,service_calls
            ,upgrade_calls
            ,Value_calls 
            ,billing_calls
            ,comms_bbcoe_calls
            ,dort_calls
            ,rtm_calls
            ,winback_calls
            ,total_calls
            ,ta_web           
)

,lookback_prod as    (
select      a.account_number
            ,a.portfolio_id
            ,a.account_type
            ,a.event_dt
            ,a.product
            ,lag(a.in_contract_flag)      over (partition by a.account_number, a.product order by a.event_dt) as prev_in_contract_flag
            ,lag(a.active_flag)           over (partition by a.account_number, a.product order by a.event_dt) as prev_active_flag
            ,lag(a.rack_rate)             over (partition by a.account_number, a.product order by a.event_dt) as prev_rack_rate
            ,lag(a.in_contract_discount)  over (partition by a.account_number, a.product order by a.event_dt) as prev_in_contract_discount
            ,lag(a.software_offer)        over (partition by a.account_number, a.product order by a.event_dt) as prev_software_offer
            ,lag(a.net_revenue)           over (partition by a.account_number, a.product order by a.event_dt) as prev_net_revenue
            ,lag(a.contract_end_date)     over (partition by a.account_number, a.product order by a.event_dt) as prev_contract_end_date
            ,lag(a.tv_latest_order_rtm_1) over (partition by a.account_number, a.product order by a.event_dt) as prev_tv_latest_order_rtm_1
            ,lag(a.tv_latest_acquisition_rtm_1) over (partition by a.account_number, a.product order by a.event_dt) as prev_tv_latest_acquisition_rtm_1
            ,lag(a.ta_calls)              over (partition by a.account_number, a.product order by a.event_dt) as prev_ta_calls
            ,lag(a.platform_calls)        over (partition by a.account_number, a.product order by a.event_dt) as prev_platform_calls
            ,lag(a.service_calls)         over (partition by a.account_number, a.product order by a.event_dt) as prev_service_calls
            ,lag(a.upgrade_calls)         over (partition by a.account_number, a.product order by a.event_dt) as prev_upgrade_calls
            ,lag(a.Value_calls)           over (partition by a.account_number, a.product order by a.event_dt) as prev_Value_calls
            ,lag(a.billing_calls)         over (partition by a.account_number, a.product order by a.event_dt) as prev_billing_calls
            ,lag(a.comms_bbcoe_calls)     over (partition by a.account_number, a.product order by a.event_dt) as prev_comms_bbcoe_calls
            ,lag(a.dort_calls)            over (partition by a.account_number, a.product order by a.event_dt) as prev_dort_calls
            ,lag(a.rtm_calls)             over (partition by a.account_number, a.product order by a.event_dt) as prev_rtm_calls
            ,lag(a.winback_calls)         over (partition by a.account_number, a.product order by a.event_dt) as prev_winback_calls
            ,lag(a.total_calls)           over (partition by a.account_number, a.product order by a.event_dt) as prev_total_calls
            ,lag(a.ta_web)                over (partition by a.account_number, a.product order by a.event_dt) as prev_ta_web            
            ,lag(a.net_revenue)           over (partition by a.portfolio_id order by a.event_dt) as prev_net_revenue_portfolio
from        tv_holdings_agg a
)

,lookback_acc as    (
select      a.account_number
            ,a.portfolio_id
            ,a.event_dt
            ,lag(a.tv_bundle_category)    over (partition by a.account_number order by a.event_dt) as prev_tv_bundle_category
            ,lag(a.tv_in_contract_flag)   over (partition by a.account_number order by a.event_dt) as prev_tv_in_contract_flag
            ,lag(a.tv_in_contract_flag_v2)   over (partition by a.account_number order by a.event_dt) as prev_tv_in_contract_flag_v2
            ,lag(a.tv_active_flag)        over (partition by a.account_number order by a.event_dt) as prev_tv_active_flag
            ,lag(a.tv_churned_flag)       over (partition by a.account_number order by a.event_dt) as prev_tv_churned_flag
            ,lag(a.tv_contract_end_date)  over (partition by a.account_number order by a.event_dt) as prev_tv_contract_end_date
            ,lag(a.tv_contract_start_date) over (partition by a.account_number order by a.event_dt) as prev_tv_contract_start_date
            ,lag(b.tv_bundle_category_portfolio)    over (partition by a.portfolio_id order by a.event_dt) as prev_tv_bundle_category_portfolio
            ,lag(b.tv_soip_flag)          over (partition by a.portfolio_id order by a.event_dt) as prev_tv_soip_flag
            ,lag(b.tv_any_active_flag)    over (partition by a.portfolio_id order by a.event_dt) as prev_tv_any_active_flag
from        base_only_agg a
left join   tv_port_agg b
on          a.portfolio_id = b.portfolio_id
and         a.event_dt = b.event_dt
)

select      a.*
            ,b.tv_bundle_category
            ,c.tv_bundle_category_portfolio
            ,ifnull(b.tv_in_contract_flag,0) as tv_in_contract_flag
            ,ifnull(b.tv_in_contract_flag_v2,0) as tv_in_contract_flag_v2
            ,ifnull(b.tv_active_flag,0) as tv_active_flag
            ,ifnull(b.tv_churned_flag,0) as tv_churned_flag
            ,ifnull(c.tv_soip_flag,0) as tv_soip_flag
            ,ifnull(c.tv_any_active_flag,0) as tv_any_active_flag
            ,e.prev_tv_bundle_category 
            ,e.prev_tv_bundle_category_portfolio
            ,ifnull(d.prev_in_contract_flag,0) as prev_in_contract_flag
            ,ifnull(d.prev_active_flag,0) as prev_active_flag
            ,ifnull(d.prev_rack_rate,0) as prev_rack_rate
            ,ifnull(d.prev_in_contract_discount,0) as prev_in_contract_discount
            ,ifnull(d.prev_software_offer,0) as prev_software_offer
            ,ifnull(d.prev_net_revenue,0) as prev_net_revenue
            ,ifnull(d.prev_net_revenue_portfolio,0) as prev_net_revenue_portfolio
            ,d.prev_tv_latest_order_rtm_1
            ,d.prev_tv_latest_acquisition_rtm_1
            ,ifnull(d.prev_ta_calls,0) as prev_ta_calls
            ,ifnull(d.prev_platform_calls,0) as prev_platform_calls
            ,ifnull(d.prev_service_calls,0) as prev_service_calls
            ,ifnull(d.prev_upgrade_calls,0) as prev_upgrade_calls
            ,ifnull(d.prev_Value_calls,0) as prev_Value_calls
            ,ifnull(d.prev_billing_calls,0) as prev_billing_calls
            ,ifnull(d.prev_comms_bbcoe_calls,0) as prev_comms_bbcoe_calls
            ,ifnull(d.prev_dort_calls,0) as prev_dort_calls
            ,ifnull(d.prev_rtm_calls,0) as prev_rtm_calls
            ,ifnull(d.prev_winback_calls,0) as prev_winback_calls
            ,ifnull(d.prev_total_calls,0) as prev_total_calls
            ,ifnull(d.prev_ta_web,0) as prev_ta_web
            ,ifnull(e.prev_tv_in_contract_flag,0) as prev_tv_in_contract_flag
            ,ifnull(e.prev_tv_in_contract_flag_v2,0) as prev_tv_in_contract_flag_v2
            ,ifnull(e.prev_tv_active_flag,0) as prev_tv_active_flag
            ,ifnull(e.prev_tv_churned_flag,0) as prev_tv_churned_flag
            ,ifnull(e.prev_tv_soip_flag,0) as prev_tv_soip_flag
            ,ifnull(e.prev_tv_any_active_flag,0) as prev_tv_any_active_flag
            ,ifnull(net_revenue,0) - ifnull(prev_net_revenue,0) as nr_delta
            ,ifnull(net_revenue,0) - ifnull(prev_net_revenue_portfolio,0) as nr_delta_portfolio
            ,case when a.product = 'Base TV' then sum(ifnull(net_revenue,0)) over (partition by a.account_number, a.event_dt) end as net_revenue_all_products
            ,case when a.product = 'Base TV' then sum(ifnull(prev_net_revenue,0)) over (partition by a.account_number, a.event_dt) end as prev_net_revenue_all_products
            ,case when a.product = 'Base TV' then sum(ifnull(net_revenue,0) - ifnull(prev_net_revenue,0)) over (partition by a.account_number, a.event_dt) end as nr_delta_all_products
            ,case       when ifnull(prev_tv_churned_flag,0) = 1 and ifnull(prev_tv_any_active_flag,0) = 0 and ifnull(tv_active_flag,0) = 1
                        then 'Reinstate'
                        when ifnull(prev_tv_any_active_flag,0) = 0 and ifnull(tv_active_flag,0) = 1 
                        then 'New Customer'
                        when prev_tv_bundle_category = 'Legacy' and tv_bundle_category like '%Signature%' and ifnull(prev_tv_active_flag,0) = 1 and ifnull(tv_active_flag,0) = 1
                        then 'Legacy to Signature Migration'
                        when prev_tv_bundle_category = 'Ents' and tv_bundle_category like '%Signature%' and ifnull(prev_tv_active_flag,0) = 1 and ifnull(tv_active_flag,0) = 1 
                        then 'Ents to Signature Migration'
                        when ifnull(prev_tv_soip_flag,0) = 0 and ifnull(tv_soip_flag,0) = 1 and ifnull(prev_tv_any_active_flag,0) = 1 and ifnull(tv_any_active_flag,0) = 1 
                        then 'DTH to SOIP Migration'                        
                        when ifnull(tv_churned_flag,0) = 1 and ifnull(tv_any_active_flag,0) = 0
                        then 'Churned Customer'
                        when ifnull(prev_tv_in_contract_flag_v2,0) = 0 and ifnull(tv_in_contract_flag_v2,0) = 1 and ifnull(prev_tv_active_flag,0) = 1 and ifnull(tv_active_flag,0) = 1
                        then 'TV Contract Added'
                        when ifnull(prev_tv_in_contract_flag_v2,0) = 1 and ifnull(tv_in_contract_flag_v2,0) = 0 and ifnull(prev_tv_active_flag,0) = 1 and ifnull(tv_active_flag,0) = 1
                        then 'TV Contract Ended'
                        when ifnull(prev_tv_in_contract_flag_v2,0) = 1 and ifnull(tv_in_contract_flag_v2,0) = 1 and ifnull(prev_tv_active_flag,0) = 1 and ifnull(tv_active_flag,0) = 1 and tv_contract_end_date > prev_tv_contract_end_date
                        then 'TV Recontract'
                        else 'No TV Movement'
                        end 
                        as tv_movement
            ,case       when  ifnull(prev_active_flag,0) = 0 and ifnull(active_flag,0) = 1 and (ifnull(prev_tv_active_flag,0) = 1 and ifnull(tv_active_flag,0) = 1)
                        then 'Product Upgrade'   
                        when  ifnull(prev_active_flag,0) = 1 and ifnull(active_flag,0) = 0 and (ifnull(prev_tv_active_flag,0) = 1 and ifnull(tv_active_flag,0) = 1)
                        then 'Product Downgrade'  
                        when  ifnull(abs(prev_software_offer),0) < ifnull(abs(software_offer),0) and (ifnull(prev_tv_active_flag,0) = 1 and ifnull(tv_active_flag,0) = 1) and prev_tv_bundle_category = tv_bundle_category
                        then 'Offer Starting'
                        when  ifnull(abs(prev_software_offer),0) > ifnull(abs(software_offer),0) and (ifnull(prev_tv_active_flag,0) = 1 and ifnull(tv_active_flag,0) = 1) and prev_tv_bundle_category = tv_bundle_category
                        then 'Offer Ending'
                        when  ifnull(prev_in_contract_flag,0) = 1 and ifnull(in_contract_flag,0) = 0 and (ifnull(prev_tv_active_flag,0) = 1 and ifnull(tv_active_flag,0) = 1)
                        then 'Product Contract Ending'
                        when  ifnull(prev_in_contract_flag,0) = 0 and ifnull(in_contract_flag,0) = 1 and (ifnull(prev_tv_active_flag,0) = 1 and ifnull(tv_active_flag,0) = 1)
                        then 'Product Contract Starting'
                        when  ifnull(prev_in_contract_flag,0) = 1 and ifnull(in_contract_flag,0) = 1 and ifnull(prev_tv_active_flag,0) = 1 and ifnull(tv_active_flag,0) = 1 and contract_end_date > prev_contract_end_date
                        then 'Product Recontract'
                        when  ifnull(prev_tv_active_flag,0) = 1 and ifnull(tv_active_flag,0) = 1 and pinc_flag = 1
                        then 'PINC'
                        else 'No Product Movement'
                        end 
                        as product_movement
from        tv_holdings_agg a
left join   base_only_agg b
on          a.account_number = b.account_number
and         a.portfolio_id = b.portfolio_id
and         a.event_dt = b.event_dt
left join   tv_port_agg c
on          a.portfolio_id = c.portfolio_id
and         a.event_dt = c.event_dt
left join   lookback_prod d
on          a.account_number = d.account_number
and         a.portfolio_id = d.portfolio_id
and         a.event_dt = d.event_dt
and         a.product = d.product
left join   lookback_acc e
on          a.account_number = e.account_number
and         a.portfolio_id = e.portfolio_id
and         a.event_dt = e.event_dt
where       (tv_active_flag = 1 or prev_tv_active_flag = 1)
and         a.event_dt >= '2020-01-01'
;