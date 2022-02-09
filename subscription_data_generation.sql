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
        ) as traffic_category
    from table(generator(rowcount => 100000))
),

initial_orders as (
    select
        customers_gen.*,
        decode(zipf(1, 5, random()),
            1, 'credit_card',
            2, 'paypal',
            3, 'apple_itunes',
            4, 'google_play',
            5, 'amazon_pay'
        ) as conversion_payment_processor,
        customer_created::date as conversion_date,
        'manual' as order_type,
        0 as cycle,
        zipf(1,(select count(*) from skus),random()) as conversion_sku_id
),

transactions as (

        concat_ws('_',skus.category,skus.short_desc) as sku,
        skus.category as sku_type,
        skus.all_items[0]::varchar as main_item,
        skus.all_items,
        concat(
            iff(
                skus.initial_purchase_price < skus.recurring_price,
                skus.initial_purchase_qty||left(skus.initial_purchase_unit,1)||'-trial-',
                ''
            ),
            concat_ws('-',
                skus.initial_purchase_qty||left(skus.initial_purchase_unit,1),
                skus.recurring_price*skus.recurring_qty,
                skus.category
            )
        ) as term,
        term as extended_term
from
    customers_gen
    left join skus
        on (select zipf(1,(select count(*) from skus),random())) = skus.id
)

select *

from
    initial_orders_gen
;
,

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

from final
)

select * from column_fills;
