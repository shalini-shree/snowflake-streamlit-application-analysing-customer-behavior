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



st.set_page_config(layout='wide')
st.title("Dashboard")
querylist = ["Query1","Query2","Query3"]

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