# subscription_data_generation.sql

## Input parameters

* Set the number of customers for the simulation.

```sql
set new_customers = 250000;
```
* Set the cross-sell rate.

```sql
set cross_sell_rate = .125;
```
* Calculate cross-sell sales qty.

```sql
set cross_sell_count = $new_customers * $cross_sell_rate;
```

## Data generation

```sql
with

skus as (
    -- create fictitious skus
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
    -- create fictitious customers with randomly assigned attributes.
    -- customer count based on `new_customers` input parameter.
    select
        row_number() over (order by seq4())::number as customer_id,
        decode(zipf(1.5,3,random()),
            1, 'The Data Driven Co.',
            2, 'iheartdata.co',
            3, 'xiirxiis'
        ) as customer_brand,
        decode(zipf(5,3,random()),
            1, 'active',
            2, 'locked',
            3, 'disabled'
        ) as customer_status,
        dateadd(day,-zipf(.1,731,random()),'2022-01-01') as acquisition_date,
        upper(randstr(4,zipf(.5,40,random()))) as acquisition_traffic_source,
        decode(left(acquisition_traffic_source,1),
            regexp_substr(acquisition_traffic_source,'[0-9]'), 'paid_media',
            regexp_substr(acquisition_traffic_source,'[A-D]'), 'paid_search',
            regexp_substr(acquisition_traffic_source,'[E-L]'), 'seo',
            regexp_substr(acquisition_traffic_source,'[M-S]'), 'affiliate',
            regexp_substr(acquisition_traffic_source,'[T-V]'), 'referral',
            regexp_substr(acquisition_traffic_source,'[W-Z]'), 'organic',
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

    from table(generator(rowcount => $new_customers))

),

initial_orders as (
    -- simulate a first order and order attributes for each customer
    select
        customers_gen.*,
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
                    'trial-'
                ),
                ''
            ),
            concat_ws('-',
                skus.recurring_qty||left(skus.recurring_unit,1),
                skus.recurring_price,
                skus.category
            )
        ) as acquisition_sku_terms,
        'manual' as order_type,
        acquisition_sku_id as order_sku_id,
        acquisition_sku as order_sku,
        acquisition_sku_type as order_sku_type,
        acquisition_sku_terms as order_sku_terms,
        acquisition_sku_main_item as order_sku_main_item,
        acquisition_sku_all_items as order_sku_all_items

    from customers_gen
         join skus on skus.id = customers_gen.acquisition_sku_id
),

initial_subscriptions as (
    -- add subscription attributes for recurring skus
    select initial_orders.*,
        case when recurring_price is not null then seq4() end as subscription_id,
        case when recurring_price is not null then acquisition_date end as subscription_created,
        case when recurring_price is not null then zipf(2,24,random())-1 else 0 end as subscription_max_cycle,
        case when recurring_price is not null then 'new' end as subscription_type,
        case initial_purchase_unit
            when 'day'
                then dateadd(day,initial_purchase_qty,subscription_created)
            when 'month'
                then dateadd(month,initial_purchase_qty,subscription_created)
            when 'year'
                then dateadd(year,initial_purchase_qty,subscription_created)
        end as subscription_rebill_start_date,
        case recurring_unit
            when 'day'
                then dateadd(day,recurring_qty*subscription_max_cycle,subscription_rebill_start_date)
            when 'month'
                then dateadd(month,recurring_qty*subscription_max_cycle,subscription_rebill_start_date)
            when 'year'
                then dateadd(year,recurring_qty*subscription_max_cycle,subscription_rebill_start_date)
         end as subscription_canceled,
         case
            when subscription_canceled < current_date then 'cancelled'
            when subscription_canceled >= current_date then 'active'
            when recurring_unit is not null then 'active'
         end as subscription_status

    from initial_orders
         join skus
           on skus.id = initial_orders.order_sku_id
),

initial_transactions as (
    -- simulate initial and recurring orders and cross_transactions
    -- note: transactions are 1:1 with orders in this model
    select
        initial_subscriptions.*,
        order_cycle,
        case
            when order_cycle = 0 then acquisition_date
            when recurring_unit = 'day'
                then dateadd(day,recurring_qty*(order_cycle-1),subscription_rebill_start_date)
            when recurring_unit = 'month'
                then dateadd(month,recurring_qty*(order_cycle-1),subscription_rebill_start_date)
            when recurring_unit = 'year'
                then dateadd(year,recurring_qty*(order_cycle-1),subscription_rebill_start_date)
        end as transaction_created,
        case order_cycle
            when 0 then initial_purchase_price
            else recurring_price
        end as transaction_amount

    from
        initial_subscriptions
        left join (
            select (row_number() over (order by null))-1 as order_cycle
            from table(generator(rowcount => 24))
        ) cycles
               on initial_subscriptions.subscription_max_cycle >= cycles.order_cycle
        left join skus
               on initial_subscriptions.order_sku_id = skus.id

),

cross_transactions as (
  -- simulate cross-sale orders and transactions
  select
      customer_id,
      customer_brand,
      customer_status,
      acquisition_date,
      acquisition_traffic_source,
      acquisition_traffic_category,
      acquisition_sku_id,
      acquisition_payment_type,
      acquisition_sku,
      acquisition_sku_type,
      acquisition_sku_main_item,
      acquisition_sku_all_items,
      acquisition_sku_terms,
      order_type,
      order_sku_id,
      'cross' as order_sku,
      case uniform(1,3,random())
          when 1 then 'Tableau'
          when 2 then 'Power BI'
          when 3 then 'Looker'
      end as order_sku_type,
      order_sku_terms,
      order_sku_main_item,
      order_sku_all_items,
      subscription_id+0.1,
      subscription_created,
      subscription_max_cycle,
      'cross' as subscription_type,
      subscription_rebill_start_date,
      subscription_canceled,
      subscription_status,
      order_cycle,
      transaction_created,
      transaction_amount

from initial_transactions
limit $cross_sell_count
),

transactions as (
    -- combine initial sales and cross-sales data
    select * from initial_transactions
    union all
    select * from cross_transactions

),

add_primary_keys as (
    -- add unique identifiers for transactions and orders
    select *,

        row_number() over (order by transaction_created) as order_id,
        order_id as transaction_id,
        'authorization' as transaction_type

    from transactions

),

final as (
    -- final cte (helpful for debugging)
    select * from add_primary_keys

)

select * from final;
```
