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


def plot_query_1(data):
    fig = px.bar(data,x='w_warehouse_name',y='perc',
                 title="Query1 visualisation")
    fig.update_layout(width=400,height=500,xaxis_title='Warehouse Name',yaxis_title='Percentage Change')
    st.plotly_chart(fig)

def plot_query_2(data):
    fig = px.bar(data,x='qoh',y='i_product_name',
                 title="Query2 visualisation")
    fig.update_layout(width=800,height=700,xaxis_title='Average Quality on Hand',yaxis_title='Product Name')
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
        dailymonthlyseq = st.slider("Choose the Daily Monthly Sequence to roll data",0,2500)
        results = fetch_query2(dailymonthlyseq)
        if results.empty:
            st.warning("Choose the correct value. No Visualisation available for chosen value.")
        else:
            st.write(results)
with c2:
    if query_output == "Query1":
        plot_query_1(results)
    elif query_output == "Query2":
        plot_query_2(results)