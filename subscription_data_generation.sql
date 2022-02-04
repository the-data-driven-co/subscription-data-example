--create table customer_data as (
    select
        row_number() over (order by seq4())::number as customer_id,
        case zipf(5,3,random())
            when 1 then 'active'
            when 2 then 'locked'
            when 3 then 'disabled'
        end as customer_status,
        case zipf(1.5,3,random())
            when 1 then 'Snowflake'
            when 2 then 'Tableau'
            when 3 then 'DataRobot'
        end as brand,
        randstr(4,zipf(.5,40,random())) as traffic_source,
        case zipf(1, 5, random())
            when 1 then 'WorldPay'
            when 2 then 'Paypal'
            when 3 then 'Apple iTunes'
            when 4 then 'Google Play'
            when 5 then 'Amazon Pay'
        end as conversion_payment_processor,
        dateadd(day,-zipf(.03,1096,random()),'2021-12-31') as customer_created,
        customer_created::date as conversion_date,





    from table(generator(rowcount => 100)) as customer_id
--)
