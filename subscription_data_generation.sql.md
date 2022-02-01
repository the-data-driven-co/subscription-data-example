
# create empty table

```sql
	create table subscription_data_example (
	    id number,
		parent_id number,
		customer_id number,
		order_id number,
		subscription_id number,
		subscription_parent_id number,
		first_sub_cancel_annotation varchar,
		"Cancel Agent: First" varchar,
		last_sub_cancel_annotation varchar,
		"Cancel Agent: Last" varchar,
		type varchar,
		sku varchar,
		order_type varchar,
		order_status varchar,
		customer_status varchar,
		subscription_status varchar,
		itemized_transaction_amount number,
		sku_amount number,
		amount number,
		sku_type varchar,
		transaction_created date,
		est_billing_date date,
		parent_est_billing_date date,
		transaction_timestamp timestamp,
		brand varchar,
		traffic_source varchar,
		conversion_payment_processor varchar,
		transaction_payment_processor varchar,
		customer_created_date date,
		conversion_date date,
		card_bin varchar,
		cycle varchar,
		conversion_sku varchar,
		conversion_sku_type varchar,
		term varchar,
		extended_term varchar,
		main_item varchar,
		all_items varchar,
		conv_term varchar,
		conv_extended_term varchar,
		conv_main_item varchar,
		conv_all_items varchar,
		subscription_created date,
		subscription_canceled date,
		sub_created_dt timestamp,
		sub_canceled_dt timestamp,
		sub_tree_id number,
		sub_tree_created date,
		sub_tree_canceled date,
		sub_tree_created_dt timestamp,
		sub_tree_canceled_dt timestamp,
		sub_tree_first_rebill_date date,
		sub_tree_type varchar,
		phone varchar,
		traffic_category varchar,
		subscription_type varchar,
		auth_charge_amount number,
		auth_reversal_amount number,
		auth_total_amount number,
		litle_message varchar,
		guardian_activated_date date,
		customers_report_date date,
		possible_report_date date,
		subtheme varchar,
		cake_traffic_source varchar,
		cake_traffic_source_name varchar,
		cake_traffic_category varchar,
		cake_sub_id_1 varchar,
		cake_sub_id_2 varchar,
		cake_sub_id_3 varchar,
		cake_sub_id_4 varchar,
		cake_sub_id_5 varchar,
		"Family Tree Created" date,
		"Chargeback Reason Code" varchar,
		"Chargeback/Fraud" varchar,
		"Fraud Flag" boolean,
		"Sift Customer" boolean,
		"Re-Sub Affiliate Name" varchar,
		"Re-Sub Affiliate Code" varchar,
		"Re-Sub Traffic Category" varchar,
		abnormal_customer_label varchar,
		abnormal_customer_fraud boolean,
		abnomal_customer_report_views boolean,
		abnormal_customer_ip_addresses boolean,
		abnormal_customer_card_names boolean,
		latest_conversion_date date,
		latest_conv_main_item varchar,
		latest_conv_extended_term varchar,
		latest_conv_sku_type varchar
	);
```

# generate customer data 

## insert customer ids from TPCH benchmark dataset

```sql
	insert into subscription_data_example(customer_id)
	    select c_custkey from snowflake_sample_data.tpch_sf1.customer;
```
## add random customer acquisition date

insert into subscription_data_example(conversion_date)
    select 
        dateadd(
          'day',
          uniform(0,1096, random()),
          '2019-01-01'::date
         )
     from
        subscription_data_example;

## add initial subscription data to customers

### generate subscription_ids

	with subscriptions as (
		select 
    		row_number() over (order by seq4()) as subscription_id
		from
    		subscription_data_example
    	);

-- 	add subscription terms with simulated distribution of subscription plan types
-- 	NULL will be for non-subcription products


        case ROUND(normal(3,1,random()))
            when 0 then NULL
            when 1 then '7d-trial-30d-11.99-phone'
            when 2 then '30d-9.99-phone'
            when 3 then '30d-28.99-background'
            when 4 then '60d-35.99-background'
            else '7d-trial-30d-32.99-background'
        end as term,
        case
            when term ilike '%trial%'
                then 1
        end
    from
        subscription_data_example;

 

-- insert into subscription_data_example(subscription_id, term, extended_term, subscription_created, sub_tree_created)


/*** NOTES ***/
--  customer
    --  conversion
    --  initial subscription
        --  transactions
        --  cross-sell subscriptions
            -- transactions
        --  resubscribes
            -- latest conversion date
            -- transactions
        -- upsell transactions