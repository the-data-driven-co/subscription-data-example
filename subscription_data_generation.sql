with
skus as (
    select
        row_number() over (order by null) as id,
        $1 as category,
        $2 as short_desc,
        parse_json($3) as all_items,
        $4 as initial_purchase_price,
        $5 as initial_purchase_unit,
        $6 as initial_purchase_qty,
        $7 as recurring_price,
        $8 as recurring_unit,
        $9 as recurring_qty
    from values
        ( 'tableau', 'creator_subscription', '["desktop","publish","view"]', 0.00, 'day', 14, 840.00, 'year', 1 ),
        ( 'powerbi', 'pro_subscription', '["publish","view"]', 0.00, 'day', 60, 120.00, 'year', 1 ),
        ( 'tableau', 'explorer_subscription', '["publish","view"]', 480.00, 'year', 1, 480.00, 'year', 1 ),
        ( 'looker', 'standard_user', '["publish","view"]', 799.00, 'year', 1, 799.00, 'year', 1 ),
        ( 'looker', 'dev_user', '["model","publish","view"]', 1665.00, 'year', 1, 1665.00, 'year', 1 ),
        ( 'powerbi', 'premium_subscription', '["model","publish","view"]', 240.00, 'year', 1, 240.00, 'year', 1 ),
        ( 'looker', 'view_user', '["view"]', 400.00, 'year', 1, 400.00, 'year', 1 ),
        ( 'tableau', 'viewer_subscription', '["view"]', 120.00, 'year', 1, 120.00, 'year', 1 ),
        ( 'tableau', 'desktop_license_only', '["desktop"]', 800.00, 'lifetime', 1, null, null, null ),
        ( 'tableau', 'desktop_updates', '["desktop_updates"]', 400.00, 'year', 1, 400.00, 'year', 1 )
),

customers_gen as (
    select
        row_number() over (order by seq4())::number as customer_id,
        dateadd(day,-zipf(.03,1096,random()),'2021-12-31') as customer_created,
        decode(zipf(5,3,random()),
            1, 'active',
            2, 'locked',
            3, 'disabled'
        ) as customer_status,
        decode(zipf(1.5,3,random()),
            1, 'thedatadriven_co',
            2, 'iheartdata_co',
            3, 'xiirxiis_analytics'
        ) as brand,
        upper(randstr(4,zipf(.5,40,random()))) as traffic_source,
        decode(left(traffic_source,1),
            regexp_substr(traffic_source,'[0-9]'), 'paid_media',
            regexp_substr(traffic_source,'[A-D]'), 'paid_search',
            regexp_substr(traffic_source,'[E-L]'), 'seo',
            regexp_substr(traffic_source,'[M-S]'), 'affiliate',
            regexp_substr(traffic_source,'[T-V]'), 'referral',
            regexp_substr(traffic_source,'[W-Z]'), 'organic',
            NULL, 'organic'
        ) as traffic_category,
        customer_created::date as conversion_date,
        zipf(1,10,random()) as conversion_sku_id,
        decode(zipf(1, 5, random()),
            1, 'credit_card',
            2, 'paypal',
            3, 'apple_itunes',
            4, 'google_play',
            5, 'amazon_pay'
        ) as conversion_payment_processor
    from table(generator(rowcount => 100000))
),

initial_orders as (
    select
        customers_gen.*,
        'manual' as order_type,
        concat_ws('_',skus.category,skus.short_desc) as conversion_sku,
        skus.category as conversion_sku_type,
        skus.all_items[0]::varchar as conv_main_item,
        skus.all_items conv_all_items,
        concat(
            iff(
                skus.initial_purchase_price < skus.recurring_price,
                skus.initial_purchase_qty||left(skus.initial_purchase_unit,1)||'-trial-',
                ''
            ),
            concat_ws('-',
                skus.recurring_qty||left(skus.recurring_unit,1),
                skus.recurring_price*skus.recurring_qty,
                skus.category
            )
        ) as conv_term,
        conv_term as conv_extended_term,
        conversion_sku_id as sku_id,
        conversion_sku as sku,
        conversion_sku_type as sku_type,
        conv_term as term,
        conv_extended_term as extended_term,
        conv_main_item as main_item,
        conv_all_items as all_items

    from customers_gen
         join skus on skus.id = customers_gen.conversion_sku_id
),

initial_subscriptions as (
    select initial_orders.*,
        case when recurring_price is not null then seq4() end as subscription_id,
        case when recurring_price is not null then conversion_date end as subscription_created,
        case when recurring_price is not null then zipf(.9,24,random())-1 else 0 end as max_cycle,
        case initial_purchase_unit
            when 'day'
                then dateadd(day,initial_purchase_qty,subscription_created)
            when 'month'
                then dateadd(month,initial_purchase_qty,subscription_created)
            when 'year'
                then dateadd(year,initial_purchase_qty,subscription_created)
        end as sub_tree_first_rebill_date,
        case recurring_unit
            when 'day'
                then dateadd(day,recurring_qty*max_cycle,sub_tree_first_rebill_date)
            when 'month'
                then dateadd(month,recurring_qty*max_cycle,sub_tree_first_rebill_date)
            when 'year'
                then dateadd(year,recurring_qty*max_cycle,sub_tree_first_rebill_date)
         end as subscription_canceled,
         case
            when subscription_canceled < current_date then 'cancelled'
            when subscription_canceled >= current_date then 'active'
            when recurring_unit is not null then 'active'
         end as subscription_status,
         subscription_id as sub_tree_id,
         subscription_created as sub_tree_created,
         subscription_canceled as sub_tree_canceled,
         case when recurring_unit is not null then 'new' end as sub_tree_type,
         sub_tree_type as subscription_type
    from initial_orders
         join skus
           on skus.id = initial_orders.sku_id
),

transactions as (
    select
        initial_subscriptions.*,
        cycle,
        case cycle
            when 0 then initial_purchase_price
            else recurring_price
        end as itemized_transaction_amount,
        case
            when cycle = 0 then customer_created
            when recurring_unit = 'day'
                then dateadd(day,recurring_qty*cycle-1,sub_tree_first_rebill_date)
            when recurring_unit = 'month'
                then dateadd(month,recurring_qty*cycle-1,sub_tree_first_rebill_date)
            when recurring_unit = 'year'
                then dateadd(year,recurring_qty*cycle-1,sub_tree_first_rebill_date)
        end as transaction_created

    from
        initial_subscriptions
        left join (
            select row_number() over (order by null)-1 as cycle
            from table(generator(rowcount => 24))
        ) cycles
               on max_cycle >= cycles.cycle
        left join skus
               on skus.id = sku_id

),

column_fills as (
select *,
--customer
    to_varchar(
        uniform(2000000000,9999999999,random()),
        '999-999-9999'
    ) as phone,
    '2021-01-01'::date as guardian_activated_date,
    '2021-01-01'::date as customers_report_date,
    '2021-01-01'::date as possible_report_date,
    'data' as subtheme,
    traffic_source as cake_traffic_source,
    traffic_source as cake_traffic_source_name,
    traffic_category as cake_traffic_category,
    'keyword' as cake_sub_id_1,
    'adposition' as cake_sub_id_2,
    'placement' as cake_sub_id_3,
    'creative' as cake_sub_id_4,
    'landing page' as cake_sub_id_5,
    '2021-01-01'::date as "Family Tree Created",
    False as "Sift Customer",
    'normal' as abnormal_customer_label,
    False as abnormal_customer_fraud,
    False as abnormal_customer_report_views,
    False as abnormal_customer_ip_addresses,
    False as abnormal_customer_card_names,
--order
    row_number() over (order by transaction_created) as order_id,
    'complete' as order_status,
    0 as sku_amount,
-- subscription
    null::varchar as first_sub_cancel_annotation,
    null::varchar as "Cancel Agent: First",
    null::varchar as last_sub_cancel_annotation,
    null::varchar as "Cancel Agent: Last",
    current_timestamp() as sub_created_dt,
    current_timestamp() as sub_canceled_dt,
    current_timestamp() as sub_tree_created_dt,
    current_timestamp() as sub_tree_canceled_dt,
    null::varchar as "Re-Sub Affiliate Name",
    null::varchar as "Re-Sub Affiliate Code",
    null::varchar as "Re-Sub Traffic Category",
    null::number as subscription_parent_id,
-- transaction
    order_id as id,
    'authorization' as type,
    itemized_transaction_amount as amount,
    transaction_created as est_billing_date,
    transaction_created as transaction_timestamp,
    conversion_payment_processor as transaction_payment_processor,
    uniform(1000,9999,random()) as card_bin,
    0 as auth_charge_amount,
    0 as auth_reversal_amount,
    0 as auth_total_amount,
    randstr(10,random()) as litle_message,
    randstr(4,random()) as "Chargeback Reason Code",
    'no' as "Chargeback/Fraud",
    False as "Fraud Flag",
    null:number parent_id,
    null:date as parent_est_billing_date
from transactions
)

select * from column_fills;
