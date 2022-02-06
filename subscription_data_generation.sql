--create table customer_data as (
    select
        row_number() over (order by seq4())::number as customer_id,  --table function, sequence
        decode(zipf(5,3,random()), --zipf, random, decode
            1, 'Active',
            2, 'Locked',
            3, 'Disabled'
        ) as customer_status,
        decode(zipf(1.5,3,random()),
            1, 'Snowflake',
            2, 'Tableau',
            3, 'DataRobot'
        ) as brand,
        upper(randstr(4,zipf(.5,40,random()))) as traffic_source, --upper, randstr
        decode(zipf(1, 5, random()),
            1, 'Credit Card',
            2, 'Paypal',
            3, 'Apple iTunes',
            4, 'Google Play',
            5, 'Amazon Pay'
        ) as conversion_payment_processor,
        dateadd(day,-zipf(.03,1096,random()),'2021-12-31') as customer_created,
        customer_created::date as conversion_date,
        decode(zipf(2,5,random()),
            1, 'full_suite_subscription',
            2, 'cloud_only_subscription',
            3, 'desktop_only_subscription',
            4, 'desktop_license_annual_updates',
            5, 'desktop_license_lifetime_updates'
        ) as conversion_sku,
        initcap(
            regexp_substr(
                conversion_sku, 'subscription|license'
            )
        ) as conversion_sku_type,
        case conversion_sku_type
            when 'subscription' then
                decode(zipf(1,3,random()),
                    1, 'monthly_billing',
                    2, 'annual_billing',
                    3, '30d_trial_monthly_billing'
                )
        end as conv_term,
        conv_term as extended_term,
        regexp_substr(conversion_sku,'(suite|cloud|desktop)') as conversion_main_item,
        case conversion_main_item
            when 'suite' then array_construct('cloud','desktop')
            else array_construct(conversion_main_item)
        end as conversion_all_items,
        to_varchar(
            uniform(2000000000,9999999999,random()),
            '999-999-9999'
        ) as phone,
        decode(left(traffic_source,1),
            regexp_substr(traffic_source,'[0-9]'), 'Paid Media',
            regexp_substr(traffic_source,'[A-D]'), 'Paid Search',
            regexp_substr(traffic_source,'[E-L]'), 'SEO',
            regexp_substr(traffic_source,'[M-S]'), 'Affiliate',
            regexp_substr(traffic_source,'[T-V]'), 'Referral',
            regexp_substr(traffic_source,'[W-Z]'), 'Organic',
            NULL, 'Organic'
        ) as traffic_category,




    from table(generator(rowcount => 100)) as customer_id
--)
