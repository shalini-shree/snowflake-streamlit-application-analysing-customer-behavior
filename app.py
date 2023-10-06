import streamlit as st
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd
import os
import plotly.express  as px
import datetime as dt

load_dotenv()

PARENT_DATABASE_NAME = "SNOWFLAKE_SAMPLE_DATA"  
NESTED_DATABASE = "TPCDS_SF10TCL"

def fetch_query1(sales_date):

    engine = create_engine(
        'snowflake://{user}:{password}@{account_identifier}/'.format(
            user=os.environ.get('user'),
            password=os.environ.get('password'),
            account_identifier=os.environ.get('account_identifier'),
        )
    )
    connection = engine.connect()
    try:
        query = f"""
        select  w_warehouse_name, i_item_id, inv_before,inv_after, 
        case when inv_before > 0 
                    then inv_after / inv_before 
                    else null
                    end as perc
        from(select w_warehouse_name
                    ,i_item_id
                    ,sum(case when (cast(d_date as date) < '{sales_date}')
                            then inv_quantity_on_hand 
                            else 0 end) as inv_before
                    ,sum(case when (cast(d_date as date) >= '{sales_date}')
                            then inv_quantity_on_hand 
                            else 0 end) as inv_after
        from {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.inventory
            ,{PARENT_DATABASE_NAME}.{NESTED_DATABASE}.warehouse
            ,{PARENT_DATABASE_NAME}.{NESTED_DATABASE}.item
            ,{PARENT_DATABASE_NAME}.{NESTED_DATABASE}.date_dim
        where i_item_sk          = inv_item_sk
            and inv_warehouse_sk   = w_warehouse_sk
            and inv_date_sk    = d_date_sk
            and d_date between dateadd(day,-30,'{sales_date}')
            and dateadd(day,30,'{sales_date}')
        group by w_warehouse_name, i_item_id) x
        where (case when inv_before > 0 
                    then inv_after / inv_before
                    else null
                    end) between 2.0/3.0 and 3.0/2.0
        order by w_warehouse_name
                ,i_item_id
        limit 100;

        """
        results = pd.read_sql(query,connection)
        return results
    finally:
        connection.close()
        engine.dispose()

def fetch_query2(dms):

    engine = create_engine(
        'snowflake://{user}:{password}@{account_identifier}/'.format(
            user=os.getenv('user'),
            password=os.getenv('password'),
            account_identifier=os.environ.get('account_identifier'),
        )
    )
    connection = engine.connect()
    try:
        query = f"""
        select  i_product_name
             ,i_brand
             ,i_class
             ,i_category
             ,avg(inv_quantity_on_hand) qoh
       from {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.inventory
           ,{PARENT_DATABASE_NAME}.{NESTED_DATABASE}.date_dim
           ,{PARENT_DATABASE_NAME}.{NESTED_DATABASE}.item
       where inv_date_sk=d_date_sk
              and inv_item_sk=i_item_sk
              and d_month_seq = {dms}
       group by rollup(i_product_name
                       ,i_brand
                       ,i_class
                       ,i_category)
       order by qoh, i_product_name, i_brand, i_class, i_category
       limit 100;

        """
        results = pd.read_sql(query,connection)
        return results
    finally:
        connection.close()
        engine.dispose()

def fetch_query3A(year):

    engine = create_engine(
        'snowflake://{user}:{password}@{account_identifier}/'.format(
            user=os.getenv('user'),
            password=os.getenv('password'),
            account_identifier=os.environ.get('account_identifier'),
        )
    )
    connection = engine.connect()
    try:
        query = f"""
        with frequent_ss_items as 
            (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
            from {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.store_sales
                ,{PARENT_DATABASE_NAME}.{NESTED_DATABASE}.date_dim 
                ,{PARENT_DATABASE_NAME}.{NESTED_DATABASE}.item
            where ss_sold_date_sk = d_date_sk
                and ss_item_sk = i_item_sk 
                and d_year in ({year},{year}+1,{year}+2,{year}+3)
            group by substr(i_item_desc,1,30),i_item_sk,d_date
            having count(*) > 4)
            select * from frequent_ss_items
            limit 100;
        """
        results = pd.read_sql(query,connection)
        results['item_sk']=results['item_sk'].astype(str)
        return results
    finally:
        connection.close()
        engine.dispose()

def fetch_query3B(year):

    engine = create_engine(
        'snowflake://{user}:{password}@{account_identifier}/'.format(
            user=os.getenv('user'),
            password=os.getenv('password'),
            account_identifier=os.environ.get('account_identifier'),
        )
    )
    connection = engine.connect()
    try:
        query = f"""
        with max_store_sales as
        (select c_customer_sk,max(csales) tpcds_cmax 
        from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
                from {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.store_sales
                    ,{PARENT_DATABASE_NAME}.{NESTED_DATABASE}.customer
                    ,{PARENT_DATABASE_NAME}.{NESTED_DATABASE}.date_dim 
                where ss_customer_sk = c_customer_sk
                and ss_sold_date_sk = d_date_sk
                and d_year in ({year},{year}+1,{year}+2,{year}+3) 
                group by c_customer_sk)
                group by c_customer_sk
                order by c_customer_sk)
                select * from max_store_sales
                limit 100;
        """
        results = pd.read_sql(query,connection)
        results['c_customer_sk']=results['c_customer_sk'].astype(str)
        return results
    finally:
        connection.close()
        engine.dispose()

def fetch_query3C(year,top):

    engine = create_engine(
        'snowflake://{user}:{password}@{account_identifier}/'.format(
            user=os.getenv('user'),
            password=os.getenv('password'),
            account_identifier=os.environ.get('account_identifier'),
        )
    )
    connection = engine.connect()
    try:
        query = f"""
        with max_store_sales as
        (select max(csales) tpcds_cmax 
        from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
                from {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.store_sales
                    ,{PARENT_DATABASE_NAME}.{NESTED_DATABASE}.customer
                    ,{PARENT_DATABASE_NAME}.{NESTED_DATABASE}.date_dim 
                where ss_customer_sk = c_customer_sk
                and ss_sold_date_sk = d_date_sk
                and d_year in ({year},{year}+1,{year}+2,{year}+3) 
                group by c_customer_sk)),
                best_ss_customer as
                (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
                from {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.store_sales
                    ,{PARENT_DATABASE_NAME}.{NESTED_DATABASE}.customer
                where ss_customer_sk = c_customer_sk
                group by c_customer_sk
                having sum(ss_quantity*ss_sales_price) > ({top}/100.0) * (select
                *
                from
                max_store_sales))
                select * from best_ss_customer
                limit 100;
        """
        results = pd.read_sql(query,connection)
        results['c_customer_sk']=results['c_customer_sk'].astype(str)
        return results
    finally:
        connection.close()
        engine.dispose()

def fetch_query3D(year,top,month):

    engine = create_engine(
        'snowflake://{user}:{password}@{account_identifier}/'.format(
            user=os.getenv('user'),
            password=os.getenv('password'),
            account_identifier=os.environ.get('account_identifier'),
        )
    )
    connection = engine.connect()
    try:
        query = f"""
                    WITH frequent_ss_items AS 
            (
            SELECT
                SUBSTR(i_item_desc, 1, 30) AS itemdesc,
                i_item_sk AS item_sk,
                d_date AS solddate,
                COUNT(*) AS cnt
            FROM
                {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.store_sales,
                {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.date_dim,
                {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.item
            WHERE
                ss_sold_date_sk = d_date_sk
                AND ss_item_sk = i_item_sk 
                AND d_year IN ({year},{year}+1,{year}+2,{year}+3)
            GROUP BY
                SUBSTR(i_item_desc, 1, 30), i_item_sk, d_date
            HAVING
                COUNT(*) > 4
            ),
            max_store_sales AS
            (
            SELECT MAX(csales) AS tpcds_cmax 
            FROM
                (
                SELECT
                    c_customer_sk,
                    SUM(ss_quantity * ss_sales_price) AS csales
                FROM
                    {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.store_sales,
                    {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.customer,
                    {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.date_dim 
                WHERE
                    ss_customer_sk = c_customer_sk
                    AND ss_sold_date_sk = d_date_sk
                    AND d_year IN ({year},{year}+2,{year}+3,{year}+4) 
                GROUP BY
                    c_customer_sk
                )
            ),
            best_ss_customer AS
            (
            SELECT
                c_customer_sk,
                SUM(ss_quantity * ss_sales_price) AS ssales
            FROM
                {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.store_sales,
                {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.customer
            WHERE
                ss_customer_sk = c_customer_sk
            GROUP BY
                c_customer_sk
            HAVING
                SUM(ss_quantity * ss_sales_price) > ({top/100}) * (SELECT tpcds_cmax FROM max_store_sales)
            )
            SELECT
            customer_id,
            sales_type,
            SUM(sales) AS total_sales
            FROM
            (
                SELECT
                cs_bill_customer_sk AS customer_id,
                'Catalog' AS sales_type,
                SUM(cs_quantity * cs_list_price) AS sales
                FROM
                {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.catalog_sales,
                {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.date_dim 
                WHERE
                d_year = {year}
                AND d_moy = {month}
                AND cs_sold_date_sk = d_date_sk 
                AND cs_item_sk IN (SELECT item_sk FROM frequent_ss_items)
                AND cs_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
                GROUP BY
                cs_bill_customer_sk

                UNION ALL

                SELECT
                ws_bill_customer_sk AS customer_id,
                'Web' AS sales_type,
                SUM(ws_quantity * ws_list_price) AS sales
                FROM
                {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.web_sales,
                {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.date_dim 
                WHERE
                d_year = {year}
                AND d_moy = {month}
                AND ws_sold_date_sk = d_date_sk 
                AND ws_item_sk IN (SELECT item_sk FROM frequent_ss_items)
                AND ws_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
                GROUP BY
                ws_bill_customer_sk
            ) AS combined_sales
            GROUP BY
            customer_id, sales_type
            LIMIT 100;

        """
        results = pd.read_sql(query,connection)
        results['customer_id']=results['customer_id'].astype(str)
        return results
    finally:
        connection.close()
        engine.dispose()

#fetch query 7
def fetch_query7(state_params, education_status, marital_status, gender, year):
    engine = create_engine(
        'snowflake://{user}:{password}@{account_identifier}/'.format(
            user=os.environ.get('user'),
            password=os.environ.get('password'),
            account_identifier=os.environ.get('account_identifier'),
        )
    )
    connection = engine.connect()
    try:
        # Ensure that state_params is not empty
        if not state_params:
            raise ValueError("No states provided in state_params")

        # Define the parameters in the query
        state_params_str = ', '.join([f"'{state}'" for state in state_params])
        query = f"""
            SELECT  i_item_id,
                    s_state, grouping(s_state) g_state,
                    avg(ss_quantity) agg1,
                    avg(ss_list_price) agg2,
                    avg(ss_coupon_amt) agg3,
                    avg(ss_sales_price) agg4
            FROM {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.store_sales, 
                 {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.customer_demographics, 
                 {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.date_dim, 
                 {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.store, 
                 {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.item
            WHERE ss_sold_date_sk = d_date_sk AND
                  ss_item_sk = i_item_sk AND
                  ss_store_sk = s_store_sk AND
                  ss_cdemo_sk = cd_demo_sk AND
                  cd_gender = '{gender}' AND
                  cd_marital_status = '{marital_status}' AND
                  cd_education_status = '{education_status}' AND
                  d_year = {year} AND
                  s_state IN ({state_params_str})
            GROUP BY ROLLUP (i_item_id, s_state)
            ORDER BY i_item_id, s_state
            LIMIT 100;
        """
        results = pd.read_sql(query, connection)
        return results
    finally:
        connection.close()
        engine.dispose()


#fetch query 8
def fetch_query8(wholesalecost_params, couponamt_params, listprice_params):
    # Snowflake connection parameters
    snowflake_params = {
        "user": os.getenv('user'),
        "password": os.getenv('password'),
        "account_identifier": os.environ.get('account_identifier'),
    }

    # Create a Snowflake engine
    engine = create_engine(
        'snowflake://{user}:{password}@{account_identifier}/'.format(**snowflake_params)
    )
    connection = engine.connect()
    try:
        query = f"""
        SELECT *
        FROM (
            SELECT
                AVG(ss_list_price) AS B1_LP,
                COUNT(ss_list_price) AS B1_CNT,
                COUNT(DISTINCT ss_list_price) AS B1_CNTD
            FROM {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.store_sales
            WHERE (ss_list_price BETWEEN {listprice_params[0]} AND {listprice_params[0]} + 10
                OR ss_coupon_amt BETWEEN {couponamt_params[0]} AND {couponamt_params[0]} + 1000
                OR ss_wholesale_cost BETWEEN {wholesalecost_params[0]} AND {wholesalecost_params[0]} + 20)
        ) B1,
        (
            SELECT
                AVG(ss_list_price) AS B2_LP,
                COUNT(ss_list_price) AS B2_CNT,
                COUNT(DISTINCT ss_list_price) AS B2_CNTD
            FROM {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.store_sales
            WHERE (ss_list_price BETWEEN {listprice_params[1]} AND {listprice_params[1]} + 10
                OR ss_coupon_amt BETWEEN {couponamt_params[1]} AND {couponamt_params[1]} + 1000
                OR ss_wholesale_cost BETWEEN {wholesalecost_params[1]} AND {wholesalecost_params[1]} + 20)
        ) B2,
        (
            SELECT
                AVG(ss_list_price) AS B3_LP,
                COUNT(ss_list_price) AS B3_CNT,
                COUNT(DISTINCT ss_list_price) AS B3_CNTD
            FROM {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.store_sales
            WHERE (ss_list_price BETWEEN {listprice_params[2]} AND {listprice_params[2]} + 10
                OR ss_coupon_amt BETWEEN {couponamt_params[2]} AND {couponamt_params[2]} + 1000
                OR ss_wholesale_cost BETWEEN {wholesalecost_params[2]} AND {wholesalecost_params[2]} + 20)
        ) B3,
        (
            SELECT
                AVG(ss_list_price) AS B4_LP,
                COUNT(ss_list_price) AS B4_CNT,
                COUNT(DISTINCT ss_list_price) AS B4_CNTD
            FROM {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.store_sales
            WHERE (ss_list_price BETWEEN {listprice_params[3]} AND {listprice_params[3]} + 10
                OR ss_coupon_amt BETWEEN {couponamt_params[3]} AND {couponamt_params[3]} + 1000
                OR ss_wholesale_cost BETWEEN {wholesalecost_params[3]} AND {wholesalecost_params[3]} + 20)
        ) B4,
        (
            SELECT
                AVG(ss_list_price) AS B5_LP,
                COUNT(ss_list_price) AS B5_CNT,
                COUNT(DISTINCT ss_list_price) AS B5_CNTD
            FROM {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.store_sales
            WHERE (ss_list_price BETWEEN {listprice_params[4]} AND {listprice_params[4]} + 10
                OR ss_coupon_amt BETWEEN {couponamt_params[4]} AND {couponamt_params[4]} + 1000
                OR ss_wholesale_cost BETWEEN {wholesalecost_params[4]} AND {wholesalecost_params[4]} + 20)
        ) B5,
        (
            SELECT
                AVG(ss_list_price) AS B6_LP,
                COUNT(ss_list_price) AS B6_CNT,
                COUNT(DISTINCT ss_list_price) AS B6_CNTD
            FROM {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.store_sales
            WHERE (ss_list_price BETWEEN {listprice_params[5]} AND {listprice_params[5]} + 10
                OR ss_coupon_amt BETWEEN {couponamt_params[5]} AND {couponamt_params[5]} + 1000
                OR ss_wholesale_cost BETWEEN {wholesalecost_params[5]} AND {wholesalecost_params[5]} + 20)
        ) B6
        LIMIT 100;
        """
        results = pd.read_sql(query, connection)
        return results
    finally:
        connection.close()
        engine.dispose()



#fetch query 9
def fetch_query9(month, year, aggregator):
    # Define a dictionary to map aggregator names to SQL functions
    aggregator_mapping = {
        "SUM": "SUM",
        "MIN": "MIN",
        "MAX": "MAX"
    }

    # Check if the provided aggregator is valid
    if aggregator not in aggregator_mapping:
        raise ValueError("Invalid aggregator. Supported aggregators: SUM, MIN, MAX")

    # Get the corresponding SQL function
    sql_aggregator = aggregator_mapping[aggregator]

    # Snowflake connection parameters
    snowflake_params = {
        "user": os.getenv('user'),
        "password": os.getenv('password'),
        "account_identifier": os.environ.get('account_identifier'),
    }

    # Create a Snowflake engine
    engine = create_engine(
        'snowflake://{user}:{password}@{account_identifier}/'.format(**snowflake_params)
    )
    connection = engine.connect()
    try:
        query = f"""
        SELECT
            i.i_item_id,
            i.i_item_desc,
            s.s_store_id,
            s.s_store_name,
            {sql_aggregator}(ss.ss_quantity) AS store_sales_aggregator,
            {sql_aggregator}(sr.sr_return_quantity) AS store_returns_aggregator,
            {sql_aggregator}(cs.cs_quantity) AS catalog_sales_aggregator
        FROM
            {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.store_sales ss
        JOIN
            {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.date_dim d1 ON ss.ss_sold_date_sk = d1.d_date_sk
        JOIN
            {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.store_returns sr ON ss.ss_customer_sk = sr.sr_customer_sk
                AND ss.ss_item_sk = sr.sr_item_sk
                AND ss.ss_ticket_number = sr.sr_ticket_number
        JOIN
            {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.catalog_sales cs ON sr.sr_customer_sk = cs.cs_bill_customer_sk
                AND sr.sr_item_sk = cs.cs_item_sk
        JOIN
            {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.date_dim d2 ON sr.sr_returned_date_sk = d2.d_date_sk
        JOIN
            {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.date_dim d3 ON cs.cs_sold_date_sk = d3.d_date_sk
        JOIN
            {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.store s ON ss.ss_store_sk = s.s_store_sk
        JOIN
            {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.item i ON ss.ss_item_sk = i.i_item_sk
        WHERE
            d1.d_moy = {month}
            AND d1.d_year = {year}
            AND d2.d_moy BETWEEN {month} AND {month} + 6
            AND d2.d_year = {year}
            AND d3.d_year IN ({year}, {year+1}, {year+2})
        GROUP BY
            i.i_item_id,
            i.i_item_desc,
            s.s_store_id,
            s.s_store_name
        ORDER BY
            i.i_item_id,
            i.i_item_desc,
            s.s_store_id,
            s.s_store_name
        LIMIT 100;
        """
        results = pd.read_sql(query, connection)
        return results
    finally:
        connection.close()
        engine.dispose()


        


#fetch query 10
def fetch_query10(year, state):
    engine = create_engine(
        'snowflake://{user}:{password}@{account_identifier}/'.format(
            user=os.getenv('user'),
            password=os.getenv('password'),
            account_identifier=os.environ.get('account_identifier'),
        )
    )
    connection = engine.connect()
    try:
        # Add a WHERE clause to the subquery to filter by the specified state
        query = f"""
        with customer_total_return as
            (select wr_returning_customer_sk as ctr_customer_sk
            ,ca_state as ctr_state, 
 	        sum(wr_return_amt) as ctr_total_return
        from {PARENT_DATABASE_NAME}.{NESTED_DATABASE}.web_returns
            ,{PARENT_DATABASE_NAME}.{NESTED_DATABASE}.date_dim
            ,{PARENT_DATABASE_NAME}.{NESTED_DATABASE}.customer_address
        where wr_returned_date_sk = d_date_sk 
            and d_year = {year}
            and wr_returning_addr_sk = ca_address_sk 
            and ca_state = '{state}'  -- Add the WHERE clause here
        group by wr_returning_customer_sk
            ,ca_state)
        select  c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
            ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
            ,c_last_review_date,ctr_state,ctr_total_return  -- Include ctr_state here
        from customer_total_return ctr1
            ,{PARENT_DATABASE_NAME}.{NESTED_DATABASE}.customer_address
            ,{PARENT_DATABASE_NAME}.{NESTED_DATABASE}.customer
        where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
 	    from customer_total_return ctr2 
        where ctr1.ctr_state = ctr2.ctr_state)
            and ca_address_sk = c_current_addr_sk
            and ca_state = '{state}'
            and ctr1.ctr_customer_sk = c_customer_sk
        order by c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
            ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
            ,c_last_review_date,ctr_state,ctr_total_return  -- Include ctr_state here
        limit 100;
      """
        results = pd.read_sql(query, connection)
        return results
    finally:
        connection.close()
        engine.dispose()

def plot_query_1(data):
    fig = px.bar(data,x='w_warehouse_name',y='perc', color='i_item_id',
                 title="Query1 visualisation")
    fig.update_layout(width=800,height=700,xaxis_title='Warehouse Name',yaxis_title='Percentage Change')
    st.plotly_chart(fig)

def plot_query_2(data):
    fig = px.bar(data,x='qoh',y='i_product_name',
                 title="Query2 visualisation")
    fig.update_layout(width=800,height=700,xaxis_title='Average Quality on Hand',yaxis_title='Product Name')
    st.plotly_chart(fig)

def plot_query_3A(data):
    fig = px.bar(data,x='itemdesc',y='cnt',
                 title="Frequently Sold Items over selected 4 consecutive years")
    fig.update_layout(width=800,height=700,xaxis_title='Item Name',yaxis_title='Count of Items Sold more than 4X')
    st.plotly_chart(fig)

def plot_query_3B(data):
    fig = px.bar(data,x='c_customer_sk',y='tpcds_cmax',
                 title="Maximum Store Sales in period of 4 consecutive years")
    fig.update_layout(width=800,height=700,xaxis_title='Customer ID',yaxis_title='Maximum Store Sales')
    st.plotly_chart(fig)

def plot_query_3C(data):
    fig = px.treemap(data,path=['c_customer_sk'],values='ssales',color='ssales',
                 title="Best Store Customers")
    fig.update_layout(width=800,height=700,xaxis_title='Customer ID',yaxis_title='Store Sales By Customer')
    st.plotly_chart(fig)

def plot_query_3D(data):
    fig = px.bar(data,x='customer_id',y='total_sales',color='sales_type',
                 title="Compute the Total Web and Catalog sales in a Selected Month-Year by Best Store Customers")
    fig.update_layout(width=800,height=700,xaxis_title='Customer ID',yaxis_title='Catalog and Web Sales by Best Customer')
    st.plotly_chart(fig)

#plot query 7
def plot_query_7(data):
    # Filter out rows with missing or invalid values
    data = data.dropna(subset=['i_item_id', 'agg1', 's_state'])

    if data.empty:
        st.warning("No valid data to visualize.")
        return

    # Create separate bar charts for each aggregation
    fig_agg1 = px.bar(data, x='i_item_id', y='agg1', color='s_state',
                      title=" Average Quantity by Item and State",
                      labels={'i_item_id': 'Item ID', 'agg1': 'Average Quantity'})

    fig_agg2 = px.bar(data, x='i_item_id', y='agg2', color='s_state',
                      title="Average List Price by Item and State",
                      labels={'i_item_id': 'Item ID', 'agg2': 'Average List Price'})

    fig_agg3 = px.bar(data, x='i_item_id', y='agg3', color='s_state',
                      title="Average Coupon Amount by Item and State",
                      labels={'i_item_id': 'Item ID', 'agg3': 'Average Coupon Amount'})

    fig_agg4 = px.bar(data, x='i_item_id', y='agg4', color='s_state',
                      title="Average Sales Price by Item and State",
                      labels={'i_item_id': 'Item ID', 'agg4': 'Average Sales Price'})

    # Display the charts
    st.plotly_chart(fig_agg1)
    st.plotly_chart(fig_agg2)
    st.plotly_chart(fig_agg3)
    st.plotly_chart(fig_agg4)

#plot query 8
def plot_query8(data):
    # Check if the required columns exist in the DataFrame
    required_columns = ["B1_LP", "B2_LP", "B3_LP", "B4_LP", "B5_LP", "B6_LP"]
    
    # Check if all required columns are present in the DataFrame
    if all(col in data.columns for col in required_columns):
        for i in range(1, 7):
            column_name = f"B{i}_LP"
            fig = px.bar(data, x=[f"B{i}"], y=column_name, title=f"Bar Chart for {column_name}")
            st.plotly_chart(fig)
    else:
        missing_columns = [col for col in required_columns if col not in data.columns]
        st.error(f"Required columns {missing_columns} not found in the results.")

#plot query 9
def plot_query9(data, aggregator):
    # Define a custom aggregation function
    def custom_aggregator(x):
        if aggregator == "SUM":
            return np.sum(x)
        elif aggregator == "MIN":
            return np.min(x)
        elif aggregator == "MAX":
            return np.max(x)
    
    # Group by store name and apply the custom aggregation
    grouped_data = data.groupby('s_store_name').agg({
        'store_sales_aggregator': custom_aggregator,
        'store_returns_aggregator': custom_aggregator
    }).reset_index()
    
    # Rename the columns with the custom aggregator function
    grouped_data.rename(columns={
        'store_sales_aggregator': f'{aggregator} Sales',
        'store_returns_aggregator': f'{aggregator} Returns'
    }, inplace=True)
    
    # Create a bar chart
    fig = px.bar(grouped_data, x='s_store_name', y=[f'{aggregator} Sales', f'{aggregator} Returns'],
                 color_discrete_sequence=['blue', 'red'], title=f'{aggregator} Sales and Returns by Store')
    
    # Customize the chart layout
    fig.update_layout(xaxis_title='Store Name', yaxis_title=f'{aggregator} Sales/Returns',
                      legend_title='Legend')
    
    # Display the chart
    st.plotly_chart(fig)

def plot_query10(data, state):
    # Filter the data for the specified state
    filtered_data = data[data['ctr_state'] == state]

    # Create a histogram of total returns
    fig = px.histogram(filtered_data, x='ctr_total_return', nbins=30,
                       title=f'Distribution of Total Returns for Customers in {state}')
    # Customize the chart layout
    fig.update_layout(xaxis_title='Total Returns', yaxis_title='Count')
    # Display the chart
    st.plotly_chart(fig)

#stream deployment code
st.set_page_config(layout='wide')
st.title("Dashboard")
querylist = ["Query1","Query2","Query3","Query7","Query8","Query9","Query10"]

with st.sidebar:
    query_output = st.selectbox("Choose the query: ",querylist)
    

c1, c2 = st.tabs(["Data","Visualisation"])
with c1:
    if query_output == "Query1":
        start_date = dt.date(2000,1,1)
        chosen_sales_date = st.date_input("Choose the Sales Date",start_date)
        results = fetch_query1(chosen_sales_date)
        if results.empty:
            st.warning("Choose the correct value. No Visualisation available for chosen value.")
        else:
            st.write(results)
            
    elif query_output == "Query2":
        dailymonthlyseq = st.slider("Choose the Daily Monthly Sequence to roll data",0,2500,1200)
        results = fetch_query2(dailymonthlyseq)
        if results.empty:
            st.warning("Choose the correct value. No Visualisation available for chosen value.")
        else:
            st.write(results)

    elif query_output == "Query3":
        year = st.selectbox('Year',range(1990, 2021))
        results1 = fetch_query3A(year)
        results2 = fetch_query3B(year)
        toppercentile = st.selectbox("Top Percentile", range(0,100),index=50)
        results3 = fetch_query3C(year,toppercentile)
        month = st.slider("Month",1,12,2)
        finalres = fetch_query3D(year,toppercentile,month)
        if  results1.empty and results2.empty and results3.empty and finalres.empty:
            st.warning("Choose the correct value. No Visualisation available for chosen value.")
        else:
            st.write("Frequently Sold Items over selected 4 consecutive years",results1)
            st.write("Maximum Store Sales in period of 4 consecutive years",results2)
            st.write("Best Store Customers",results3)
            st.write("Compute the Total Web and Catalog sales in a Selected Month-Year by Best Store Customers",finalres)
    elif query_output == "Query7":
        # Display the Streamlit inputs
        st.title("Query 7")
        state_params = st.multiselect("Select States:", ["TN", "SD", "OH", "NM", "MI", "TX"]) or ["default_state"]
        education_status = st.selectbox("Select Education Status:", ["Primary", "Secondary", "College", "2 yr Degree", "4 yr Degree", "Unknown"])
        marital_status = st.selectbox("Select Marital Status:", ["M", "S", "D", "W", "U"])
        gender = st.selectbox("Select Gender:", ["F", "M"])
        year = st.number_input("Enter Year:", 2000)
        results = fetch_query7(state_params, education_status, marital_status, gender, year)
        st.write("Query Results:")
        st.write(results)
        if results.empty:
            st.warning("Choose the correct value. No Visualisation available for chosen value.")  # Define an empty DataFrame or provide a default value
    elif query_output == "Query8":
        # Display the Streamlit inputs
        st.title("Query 8")
        wholesalecost_params = st.text_input("Enter values for WHOLESALECOST :")
        couponamt_params = st.text_input("Enter values for COUPONAMT :")
        listprice_params = st.text_input("Enter values for LISTPRICE :")
        try:
            # Split the user input into a list of values and convert to integers
            wholesalecost_values = [int(val.strip()) for val in wholesalecost_params.split(',')]
            couponamt_values = [int(val.strip()) for val in couponamt_params.split(',')]
            listprice_values = [int(val.strip()) for val in listprice_params.split(',')]

            # Call the fetch_query8 function with parameter values
            results = fetch_query8(wholesalecost_values, couponamt_values, listprice_values)

            # Display the results
            st.write("Query Results:")
            st.write(results)
            if results.empty:
                st.warning("No results found for the given parameter values.")
        except ValueError:
            st.error("Invalid input. Please enter integer values separated by commas.")
    elif query_output == "Query9":
    # Display the Streamlit inputs
        st.title("Query 9")
        month = st.number_input("Enter MONTH:", min_value=1, max_value=12, step=1)
        year = st.number_input("Enter YEAR:", min_value=2000)
        aggregator = st.selectbox("Select Aggregator:", ["SUM", "MIN", "MAX"])

    # Automatically execute the query when any input changes
        results = fetch_query9(month, year, aggregator)
    # Display the results
        st.write("Query Results:")
        st.write(results)
        if results.empty:
            st.warning("No results found for the given parameter values.")
    # Call the plot_query9 function to generate and display the visualization
    elif query_output == "Query10":
        year = st.text_input("Enter YEAR:", key="year_input")  # Unique key for year input
        state = st.text_input("Enter STATE:", key="state_input")  # Unique key for state input

    # Check if the inputs are provided
        if year and state:
        # Call the fetch_query10 function with parameter values
            results = fetch_query10(year, state)
            st.write("Query Results:")
            st.write(results)
            if results.empty:
                st.warning("No results found for the given parameters.")       
with c2:
    if query_output == "Query1":
        plot_query_1(results)
    elif query_output == "Query2":
        plot_query_2(results)
    elif query_output == "Query3":
        plot_query_3A(results1)
        plot_query_3B(results2)
        plot_query_3C(results3)
        plot_query_3D(finalres)
    elif query_output == "Query7":
        # Check if results are defined
        if 'results' in locals() and not results.empty:
            # Plot the query results
            plot_query_7(results)
        else:
            st.warning("No results to visualize. Please execute the query in the 'Data' tab first.")
    elif query_output == "Query8":
        # Check if results8 are defined
        if 'results' in locals() and not results.empty:
            # Plot the query results
            plot_query8(results)
        else:
            st.warning("No results to visualize. Please execute the query in the 'Data' tab first.")
    elif query_output == "Query9":
        # Check if results are defined
        if 'results' in locals() and not results.empty:
            # Plot the query results
            plot_query9(results, aggregator)
        else:
            st.warning("No results to visualize. Please execute the query in the 'Data' tab first.")
    elif query_output == "Query10":
        # Check if results are defined
        if 'results' in locals() and not results.empty:
            # Plot the query results
            plot_query10(results, state)
        else:
            st.warning("No results to visualize. Please execute the query in the 'Data' tab first.")