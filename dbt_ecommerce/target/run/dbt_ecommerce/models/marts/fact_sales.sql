
      
        
            delete from "analytics_warehouse"."public"."fact_sales"
            using "fact_sales__dbt_tmp160037072847"
            where (
                
                    "fact_sales__dbt_tmp160037072847".order_id = "analytics_warehouse"."public"."fact_sales".order_id
                    and 
                
                    "fact_sales__dbt_tmp160037072847".order_item_id = "analytics_warehouse"."public"."fact_sales".order_item_id
                    
                
                
            );
        
    

    insert into "analytics_warehouse"."public"."fact_sales" ("order_id", "order_item_id", "customer_id", "product_id", "seller_id", "purchase_ts", "delivered_ts", "updated_at", "price", "freight_value", "total_item_value")
    (
        select "order_id", "order_item_id", "customer_id", "product_id", "seller_id", "purchase_ts", "delivered_ts", "updated_at", "price", "freight_value", "total_item_value"
        from "fact_sales__dbt_tmp160037072847"
    )
  