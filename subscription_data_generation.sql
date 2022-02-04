--create table customer_data as (
    select
        row_number() over (order by seq4())::number as customer_id,
        case zipf(5,3,random())
            when 1 then 'Active'
            when 2 then 'Locked'
            when 3 then 'Disabled'
        end as customer_status,
        case zipf(1.5,3,random())
            when 1 then 'Snowflake'
            when 2 then 'Tableau'
            when 3 then 'DataRobot'
        end as brand,
        randstr(4,zipf(.5,40,random())) as traffic_source,
        case zipf(1, 5, random())
            when 1 then 'Credit Card'
            when 2 then 'Paypal'
            when 3 then 'Apple iTunes'
            when 4 then 'Google Play'
            when 5 then 'Amazon Pay'
        end as conversion_payment_processor,
        dateadd(day,-zipf(.03,1096,random()),'2021-12-31') as customer_created,
        customer_created::date as conversion_date,
        case zipf(2,5,random())
            when 1 then 'full_suite_subscription'
            when 2 then 'cloud_only_subscription'
            when 3 then 'desktop_only_subscription'
            when 4 then 'desktop_license_annual_updates'
            when 5 then 'desktop_license_lifetime_updates'
        end as conversion_sku,






    from table(generator(rowcount => 100)) as customer_id
--)
