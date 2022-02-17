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
        ( 'Tableau', 'creator_subscription', '["desktop","publish","view"]', 0.00, 'day', 14, 70.00, 'day', 30 ),
        ( 'Tableau', 'creator_subscription', '["desktop","publish","view"]', 70.00, 'day', 30, 70.00, 'day', 30 ),
        ( 'Power BI', 'pro_subscription', '["publish","view"]', 0.00, 'day', 60, 9.99, 'day', 30),
        ( 'Power BI', 'pro_subscription', '["publish","view"]', 9.99, 'day', 30, 9.99, 'day', 30),
        ( 'Tableau', 'explorer_subscription', '["publish","view"]', 35.00, 'day', 30, 35.00, 'day', 30 ),
        ( 'Looker', 'standard_user', '["publish","view"]', 66.58, 'day', 30, 66.58, 'day', 30 ),
        ( 'Looker', 'dev_user', '["model","publish","view"]', 138.75, 'day', 30, 138.75, 'day', 30 ),
        ( 'Power BI', 'premium_subscription', '["model","publish","view"]', 20.00, 'day', 30, 20.00, 'day', 30 ),
        ( 'Looker', 'view_user', '["view"]', 33.33, 'day', 30, 33.33, 'day', 30 ),
        ( 'Tableau', 'viewer_subscription', '["view"]', 15.00, 'day', 30, 15.00, 'day', 30 ),
        ( 'Tableau', 'desktop_license_only', '["desktop"]', 800.00, 'lifetime', 1, null, null, null ),
        ( 'Tableau', 'desktop_updates', '["desktop_updates"]', 400.00, 'day', 365, 400.00, 'day', 365 ),
        ( 'Tableau', 'data_management', '["add-on"]', 10.00, 'day', 30, 10.00, 'day', 30 ),
        ( 'Tableau', 'resource_block', '["add-on"]', 250.00, 'day', 30, 250.00, 'day', 30 )
),

customers_gen as (
    select
        row_number() over (order by seq4())::number as customer_id,
        dateadd(day,-zipf(.1,731,random()),'2022-01-01') as acquisition_date,
        decode(zipf(5,3,random()),
            1, 'active',
            2, 'locked',
            3, 'disabled'
        ) as customer_status,
        decode(zipf(1.5,3,random()),
            1, 'The Data Driven Co.',
            2, 'iheartdata.co',
            3, 'xiirxiis'
        ) as customer_brand,
        upper(randstr(4,zipf(.5,40,random()))) as acquisition_traffic_source,
        decode(left(traffic_source,1),
            regexp_substr(traffic_source,'[0-9]'), 'paid_media',
            regexp_substr(traffic_source,'[A-D]'), 'paid_search',
            regexp_substr(traffic_source,'[E-L]'), 'seo',
            regexp_substr(traffic_source,'[M-S]'), 'affiliate',
            regexp_substr(traffic_source,'[T-V]'), 'referral',
            regexp_substr(traffic_source,'[W-Z]'), 'organic',
            NULL, 'organic'
        ) as acquisition_traffic_category,
        zipf(.5,14,random()) as acquisition_sku_id,
        decode(zipf(1, 5, random()),
            1, 'credit_card',
            2, 'paypal',
            3, 'apple_itunes',
            4, 'google_play',
            5, 'amazon_pay'
        ) as acquisition_payment_type
    from table(generator(rowcount => 250000))
),

initial_orders as (
    select
        customers_gen.*,
        'manual' as order_type,
        concat_ws('_',skus.category,skus.short_desc) as acquisition_sku,
        skus.category as acquisition_sku_type,
        skus.all_items[0]::varchar as acquisition_sku_main_item,
        skus.all_items acquisition_sku_all_items,
        concat(
            iff(
                skus.initial_purchase_price < skus.recurring_price,
                concat_ws('-',
                    skus.initial_purchase_qty||left(skus.initial_purchase_unit,1),
                    skus.initial_purchase_price,
                    'trial-',
                ''
            ),
            concat_ws('-',
                skus.recurring_qty||left(skus.recurring_unit,1),
                skus.recurring_price,
                skus.category
            )
        ) as acquisition_sku_terms,
        acquisition_sku_id as sku_id,
        acquisition_sku as sku,
        acquisition_sku_type as sku_type,
        acquisition_sku_terms as sku_terms,
        acquisition_sku_main_item as sku_main_item,
        acquisition_sku_all_items as sku_all_items

    from customers_gen
         join skus on skus.id = customers_gen.acquisition_sku_id
),

initial_subscriptions as (
    select initial_orders.*,
        case when recurring_price is not null then seq4() end as subscription_id,
        case when recurring_price is not null then acquisition_date end as subscription_created,
        case when recurring_price is not null then zipf(2,24,random())-1 else 0 end as max_cycle,
        case initial_purchase_unit
            when 'day'
                then dateadd(day,initial_purchase_qty,subscription_created)
            when 'month'
                then dateadd(month,initial_purchase_qty,subscription_created)
            when 'year'
                then dateadd(year,initial_purchase_qty,subscription_created)
        end as subscription_auto_order_start_date,
        case recurring_unit
            when 'day'
                then dateadd(day,recurring_qty*max_cycle,subscription_auto_order_start_date)
            when 'month'
                then dateadd(month,recurring_qty*max_cycle,subscription_auto_order_start_date)
            when 'year'
                then dateadd(year,recurring_qty*max_cycle,subscription_auto_order_start_date)
         end as subscription_canceled,
         case
            when subscription_canceled < current_date then 'cancelled'
            when subscription_canceled >= current_date then 'active'
            when recurring_unit is not null then 'active'
         end as subscription_status,
         case when recurring_unit is not null then 'new' end as subscription_type,
    from initial_orders
         join skus
           on skus.id = initial_orders.sku_id
),

initial_transactions as (
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
                then dateadd(day,recurring_qty*(cycle-1),sub_tree_first_rebill_date)
            when recurring_unit = 'month'
                then dateadd(month,recurring_qty*(cycle-1),sub_tree_first_rebill_date)
            when recurring_unit = 'year'
                then dateadd(year,recurring_qty*(cycle-1),sub_tree_first_rebill_date)
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

cross_transactions as (
  select
      customer_id,
      customer_created,
      customer_status,
      brand,
      traffic_source,
      traffic_category,
      acquisition_date,
      acquisition_sku_id,
      acquisition_payment_processor,
      order_type,
      acquisition_sku,
      acquisition_sku_type,
      conv_main_item,
      conv_all_items,
      conv_term,
      conv_extended_term,
      sku_id,
      'cross' as sku,
      case uniform(1,3,random())
          when 1 then 'Tableau'
          when 2 then 'Power BI'
          when 3 then 'Looker'
      end as sku_type,
      term,
      extended_term,
      main_item,
      all_items,
      subscription_id+0.1,
      subscription_created,
      max_cycle,
      sub_tree_first_rebill_date,
      subscription_canceled,
      subscription_status,
      sub_tree_id+0.1,
      sub_tree_created,
      sub_tree_canceled,
      'cross' as sub_tree_type,
      'cross' as subscription_type,
      cycle,
      itemized_transaction_amount,
      transaction_created
from initial_transactions
order by customer_id
limit 100000),

transactions as (
    select * from initial_transactions
    union
    select * from cross_transactions
),

final as (

    select *,
        row_number() over (order by transaction_created) as order_id,
        order_id as transaction_id,
        'authorization' as transaction_type

from transactions
)

select * from transactions;
