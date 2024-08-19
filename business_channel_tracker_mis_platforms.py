#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  4 16:22:32 2024

@author: akash.thakur
"""

import psycopg2
from sshtunnel import SSHTunnelForwarder
import pygsheets
import pandas as pd
from datetime import date, timedelta, datetime
import gspread_dataframe as gd
import sys
sys.path.append("/home/ec2-user/")
from Cron_files.hosts import AWSCredentials
import numpy as np
import warnings
warnings.filterwarnings("ignore")
import sys


import global_parameters as g
#import create_table_queries as ctq
#import create_table_execute as cte
#import insert_data_queries as idq
#import deletetion_values_db_time_range as dvdbtr
import validate_phone_number as vpn

def main():
    
    voltlms_query, customer_leads, application_data, sanctioned_amount_query, fetch_date_query, pledge_date_query = get_db_customer_data()
    df_customer_details = merging_customer_mfc_app_data(voltlms_query, customer_leads, application_data, sanctioned_amount_query)
    df_customer_details_fetch = merging_customer_mfc_app_data(voltlms_query, customer_leads, fetch_date_query, sanctioned_amount_query)
    df_customer_details_pledge = merging_customer_mfc_app_data(voltlms_query, customer_leads, pledge_date_query, sanctioned_amount_query)

    grouped_platforms = filter_and_group_platforms_completed(df_customer_details, 'M')
    grouped_platform_type = filter_and_group_platform_type_completed(df_customer_details, 'M')
    df_completed = pd.concat([grouped_platforms, grouped_platform_type], ignore_index=True)
    
    grouped_platforms_days = filter_and_group_platforms_completed(df_customer_details, 'D')
    grouped_platform_type_days = filter_and_group_platform_type_completed(df_customer_details, 'D')
    df_completed_days = pd.concat([grouped_platforms_days, grouped_platform_type_days], ignore_index=True)
    
    
    grouped_platforms_registered = filter_and_group_platforms_registered(df_customer_details, 'M')
    grouped_platform_type_registered = filter_and_group_platform_type_registered(df_customer_details, 'M')
    df_registered = pd.concat([grouped_platforms_registered, grouped_platform_type_registered], ignore_index=True)
    
    
    grouped_platforms_registered_fetch = filter_and_group_platforms_registered(df_customer_details_fetch, 'M')
    grouped_platform_type_registered_fetch = filter_and_group_platform_type_registered(df_customer_details_fetch, 'M')
    df_registered_fetch = pd.concat([grouped_platforms_registered_fetch, grouped_platform_type_registered_fetch], ignore_index=True)

    grouped_platforms_registered_pledge = filter_and_group_platforms_registered(df_customer_details_pledge, 'M')
    grouped_platform_type_registered_pledge = filter_and_group_platform_type_registered(df_customer_details_pledge, 'M')
    df_registered_pledge = pd.concat([grouped_platforms_registered_pledge, grouped_platform_type_registered_pledge], ignore_index=True)


    print(df_completed)
    print(df_registered)
    
    print(df_registered_fetch)
    print(df_registered_pledge)
    print(df_completed_days)
    #update_worksheet(g.secret_file, g.mis_business_channel_sheet, "test", df_customer_details)
    update_worksheet(g.secret_file, g.mis_business_channel_sheet, "grouped completed on", df_completed)
    update_worksheet(g.secret_file, g.mis_business_channel_sheet, "grouped completed on days", df_completed_days)

    update_worksheet(g.secret_file, g.mis_business_channel_sheet, "grouped created on", df_registered)
    
    update_worksheet(g.secret_file, g.mis_business_channel_sheet, "grouped fetched on", df_registered_fetch)
    update_worksheet(g.secret_file, g.mis_business_channel_sheet, "grouped pledged on", df_registered_pledge)


    
    return 



    


def update_worksheet(service_account_file, spreadsheet_id, title, dataframe):
    client = pygsheets.authorize(service_account_file=service_account_file)
    sh = client.open_by_key(spreadsheet_id)
    ws = sh.worksheet("title", title)
    
    # Clear only the values in the range of the existing data
    existing_data_range = ws.get_values(start='A1', end=(ws.rows, ws.cols), returnas='range')
    existing_data_range.clear()
    
    # Resize the worksheet if necessary
    ws.resize(dataframe.shape[0], dataframe.shape[1])
    
    # Update the data starting from the first cell
    ws.set_dataframe(dataframe, start=(1, 1))


def get_db_customer_data():
    client = pygsheets.authorize(service_account_file='/home/ec2-user/configurations_dir/dataautomation.json')

    # VOLT_LMS
    with SSHTunnelForwarder(
        (AWSCredentials.host, 22),
        ssh_username= AWSCredentials.ssh_username, 
        ssh_pkey="/home/ec2-user/configurations_dir/volt-reporting-tunnel-2.cer",
        remote_bind_address=(AWSCredentials.Voltlms.blind_address, 5432)
    ) as server:
        server.start()
        
        # Update the port in the connection parameters
        connection_params = AWSCredentials.Voltlms.params.copy()
        connection_params['port'] = server.local_bind_port

        # Now use connection_params to make the connection
        conn = psycopg2.connect(**connection_params)
        conn.set_session(readonly=True)
        print ("DB Connected")

        voltlms_query= pd.read_sql_query("""
            select 
                lead_id, total_portfolio_value as mfc_total_portfolio_value, eligible_portfolio_value as mfc_eligible_portfolio_value, 
                eligible_credit_limit as mfc_eligible_credit_limit, created_on, last_updated_on
                from leads_portfolio_data
                                """, conn) 
        conn.close() 
    
    # Voltaudits_01
    with SSHTunnelForwarder(
        (AWSCredentials.host, 22),
        ssh_username= AWSCredentials.ssh_username, 
        ssh_pkey="/home/ec2-user/configurations_dir/volt-reporting-tunnel-2.cer",
        remote_bind_address=(AWSCredentials.Voltaudits.blind_address, 5432)
    ) as server:
        server.start()
        
        # Update the port in the connection parameters
        connection_params = AWSCredentials.Voltaudits.params.copy()
        connection_params['port'] = server.local_bind_port

        # Now use connection_params to make the connection
        conn = psycopg2.connect(**connection_params)
        conn.set_session(readonly=True) 
        print ("DB Connected")

        customer_leads= pd.read_sql_query("""
            select 
                CASE WHEN cl.platform_account_id NOT IN ('6d450a88-8312-4398-b7a6-65b27bd5b980', '6a8d1b14-61fc-4b30-ad07-de4052b4f295') 
                    AND cl.platform_account_id IS NOT NULL THEN 'B2B'
                    WHEN (cl.platform_account_id IN ('6d450a88-8312-4398-b7a6-65b27bd5b980', '6a8d1b14-61fc-4b30-ad07-de4052b4f295') 
                    OR cl.platform_account_id IS NULL) AND cl.partner_account_id IS NOT NULL 
                    AND pa.partner_account_type IN ('INDIVIDUAL', NULL) THEN 'B2B2C'
                    ELSE 'B2C'
                END AS business_channel, 
                lead_id, cl.pan, cl.partner_account_id, cl.platform_account_id, cl.phone_number as mfc_phone_number, pla2.platform_name, pa.partner_name, pa.partner_account_type
                from
            (select lead_id, pan, partner_account_id, platform_account_id, phone_number 
                from customer_leads) cl
            left join
               (select account_id, platform_name from platform_accounts) pla2
            on pla2.account_id = cl.platform_account_id
            left join
                (select partner_name, account_id, partner_account_type from partner_accounts) pa
            on pa.account_id= cl.partner_account_id
                    """, conn) 
        conn.close() 
    # Voltaudits_02
    with SSHTunnelForwarder(
        (AWSCredentials.host, 22),
        ssh_username= AWSCredentials.ssh_username, 
        ssh_pkey="/home/ec2-user/configurations_dir/volt-reporting-tunnel-2.cer",
        remote_bind_address=(AWSCredentials.Voltaudits.blind_address, 5432)
    ) as server:
        server.start()
        
        # Update the port in the connection parameters
        connection_params = AWSCredentials.Voltaudits.params.copy()
        connection_params['port'] = server.local_bind_port

        # Now use connection_params to make the connection
        conn = psycopg2.connect(**connection_params)
        conn.set_session(readonly=True)
        print ("DB Connected")

        # unique identifier as account_id
        application_data= pd.read_sql_query("""
                    SELECT
                        x.application_id, z.account_holderpan as app_pan, z.account_holder_phone_number as app_phone_number,
                        y.available_portfolio_value as app_available_portfolio_value, y.eligible_portfolio_value as app_eligible_portfolio_value,
                        y.eligible_credit_limit as app_eligible_credit_limit, y.pledged_portfolio_value as app_pledged_portfolio_value,
                        y.pledged_credit_limit as app_pledged_credit_limit, x.current_step_id as app_current_step_id,
                        x.last_updated_on as app_last_updated_on, x.completed_on as completed_on, x.application_type, x.application_state,
                        x.platform_account_id, x.partner_account_id, pa.partner_account_type, coalesce(pla.platform_name, 'VOLT') platform_name, pa.partner_name,
                        CASE WHEN x.platform_account_id NOT IN ('6d450a88-8312-4398-b7a6-65b27bd5b980', '6a8d1b14-61fc-4b30-ad07-de4052b4f295') 
                        AND x.platform_account_id IS NOT NULL THEN 'B2B'
                            WHEN (x.platform_account_id IN ('6d450a88-8312-4398-b7a6-65b27bd5b980', '6a8d1b14-61fc-4b30-ad07-de4052b4f295') 
                            OR x.platform_account_id IS NULL) AND x.partner_account_id IS NOT NULL 
                            AND pa.partner_account_type IN ('INDIVIDUAL', NULL) THEN 'B2B2C'
                            ELSE 'B2C'
                        END AS business_channel, COALESCE(x.created_on, y.created_on) as created_on
                        from
                    (select 
                        application_id, account_id, current_step_id, application_state, application_type, 
                            last_updated_on,completed_on, created_on, platform_account_id, partner_account_id from credit_applications_entity 
                            where application_state != 'SUSPENDED') x
                    left join
                    (select application_id, available_portfolio_value, eligible_credit_limit, eligible_portfolio_value, pledged_credit_limit, pledged_portfolio_value, created_on
                        from credit_application_meta_data) y
                    on x.application_id = y.application_id
                    left join
                    (select account_id, account_holderpan, account_holder_phone_number from borrower_accounts) z
                    on x.account_id = z.account_id
                    left join
                    (select account_id, partner_name, partner_account_type from partner_accounts) pa
                    on pa.account_id = x.partner_account_id
                    left join
                    (select platform_name, account_id from platform_accounts) as pla
                    on pla.account_id= x.platform_account_id
                    """, conn) 
        conn.close() 
        
    # Voltaudits_02
    with SSHTunnelForwarder(
        (AWSCredentials.host, 22),
        ssh_username= AWSCredentials.ssh_username, 
        ssh_pkey="/home/ec2-user/configurations_dir/volt-reporting-tunnel-2.cer",
        remote_bind_address=(AWSCredentials.Voltaudits.blind_address, 5432)
    ) as server:
        server.start()
        
        # Update the port in the connection parameters
        connection_params = AWSCredentials.Voltaudits.params.copy()
        connection_params['port'] = server.local_bind_port

        # Now use connection_params to make the connection
        conn = psycopg2.connect(**connection_params)
        conn.set_session(readonly=True)
        print ("DB Connected")

        sanctioned_amount_query= pd.read_sql_query("""
            select  application_id , pledged_credit_limit as sanctioned_limit  from credit_application_meta_data 
                                """, conn) 
        conn.close() 

    # Voltaudits_02
    with SSHTunnelForwarder(
        (AWSCredentials.host, 22),
        ssh_username= AWSCredentials.ssh_username, 
        ssh_pkey="/home/ec2-user/configurations_dir/volt-reporting-tunnel-2.cer",
        remote_bind_address=(AWSCredentials.Voltaudits.blind_address, 5432)
    ) as server:
        server.start()
        
        # Update the port in the connection parameters
        connection_params = AWSCredentials.Voltaudits.params.copy()
        connection_params['port'] = server.local_bind_port

        # Now use connection_params to make the connection
        conn = psycopg2.connect(**connection_params)
        conn.set_session(readonly=True)
        print ("DB Connected")

        fetch_date_query= pd.read_sql_query("""
                                                   
                                                   
               SELECT
                   x.application_id, z.account_holderpan as app_pan, z.account_holder_phone_number as app_phone_number,
                   y.available_portfolio_value as app_available_portfolio_value, y.eligible_portfolio_value as app_eligible_portfolio_value,
                   y.eligible_credit_limit as app_eligible_credit_limit, y.pledged_portfolio_value as app_pledged_portfolio_value,
                   y.pledged_credit_limit as app_pledged_credit_limit, x.current_step_id as app_current_step_id,
                   x.last_updated_on as app_last_updated_on, x.completed_on as completed_on, x.application_type, x.application_state,
                   x.platform_account_id, x.partner_account_id, pa.partner_account_type, coalesce(pla.platform_name, 'VOLT') platform_name, pa.partner_name,
                   CASE WHEN x.platform_account_id NOT IN ('6d450a88-8312-4398-b7a6-65b27bd5b980', '6a8d1b14-61fc-4b30-ad07-de4052b4f295') 
                   AND x.platform_account_id IS NOT NULL THEN 'B2B'
                       WHEN (x.platform_account_id IN ('6d450a88-8312-4398-b7a6-65b27bd5b980', '6a8d1b14-61fc-4b30-ad07-de4052b4f295') 
                       OR x.platform_account_id IS NULL) AND x.partner_account_id IS NOT NULL 
                       AND pa.partner_account_type IN ('INDIVIDUAL', NULL) THEN 'B2B2C'
                       ELSE 'B2C'
                   END AS business_channel, COALESCE(x.created_on, y.created_on) as created_on
                   from
               (SELECT
          application_id,
          account_id,
          current_step_id,
          application_state,
          application_type,
          last_updated_on,
          completed_on,
          created_on,
          platform_account_id,
          partner_account_id
      FROM (
          SELECT
              caea.application_id,
              caea.account_id,
              caea.current_step_id,
              caea.application_state,
              caea.application_type,
              caea.last_updated_on,
              caea.completed_on,
              caea.created_on,
              caea.platform_account_id,
              caea.partner_account_id,
              ROW_NUMBER() OVER (PARTITION BY caea.application_id ORDER BY caea.last_updated_on DESC) AS rank
          FROM
              credit_applications_entity_audit caea
          WHERE
               caea.current_step_id = 'MF_FETCH_PORTFOLIO' and application_state != 'SUSPENDED'
      ) subquery
      WHERE
          subquery.rank = 1
      ) x
               left join
               (select application_id, available_portfolio_value, eligible_credit_limit, eligible_portfolio_value, pledged_credit_limit, pledged_portfolio_value, created_on
                   from credit_application_meta_data) y
               on x.application_id = y.application_id
               left join
               (select account_id, account_holderpan, account_holder_phone_number from borrower_accounts) z
               on x.account_id = z.account_id
               left join
               (select account_id, partner_name, partner_account_type from partner_accounts) pa
               on pa.account_id = x.partner_account_id
               left join
               (select platform_name, account_id from platform_accounts) as pla
               on pla.account_id= x.platform_account_id                                    
                                                   

                                                   """, conn) 
        conn.close() 
        
    # Voltaudits_02
    with SSHTunnelForwarder(
        (AWSCredentials.host, 22),
        ssh_username= AWSCredentials.ssh_username, 
        ssh_pkey="/home/ec2-user/configurations_dir/volt-reporting-tunnel-2.cer",
        remote_bind_address=(AWSCredentials.Voltaudits.blind_address, 5432)
    ) as server:
        server.start()
        
        # Update the port in the connection parameters
        connection_params = AWSCredentials.Voltaudits.params.copy()
        connection_params['port'] = server.local_bind_port

        # Now use connection_params to make the connection
        conn = psycopg2.connect(**connection_params)
        conn.set_session(readonly=True)
        print ("DB Connected")

        pledge_date_query= pd.read_sql_query("""
                                                   
                                                   
               SELECT
                   x.application_id, z.account_holderpan as app_pan, z.account_holder_phone_number as app_phone_number,
                   y.available_portfolio_value as app_available_portfolio_value, y.eligible_portfolio_value as app_eligible_portfolio_value,
                   y.eligible_credit_limit as app_eligible_credit_limit, y.pledged_portfolio_value as app_pledged_portfolio_value,
                   y.pledged_credit_limit as app_pledged_credit_limit, x.current_step_id as app_current_step_id,
                   x.last_updated_on as app_last_updated_on, x.completed_on as completed_on, x.application_type, x.application_state,
                   x.platform_account_id, x.partner_account_id, pa.partner_account_type, coalesce(pla.platform_name, 'VOLT') platform_name, pa.partner_name,
                   CASE WHEN x.platform_account_id NOT IN ('6d450a88-8312-4398-b7a6-65b27bd5b980', '6a8d1b14-61fc-4b30-ad07-de4052b4f295') 
                   AND x.platform_account_id IS NOT NULL THEN 'B2B'
                       WHEN (x.platform_account_id IN ('6d450a88-8312-4398-b7a6-65b27bd5b980', '6a8d1b14-61fc-4b30-ad07-de4052b4f295') 
                       OR x.platform_account_id IS NULL) AND x.partner_account_id IS NOT NULL 
                       AND pa.partner_account_type IN ('INDIVIDUAL', NULL) THEN 'B2B2C'
                       ELSE 'B2C'
                   END AS business_channel, COALESCE(x.created_on, y.created_on) as created_on
                   from
               (SELECT
          application_id,
          account_id,
          current_step_id,
          application_state,
          application_type,
          last_updated_on,
          completed_on,
          created_on,
          platform_account_id,
          partner_account_id
      FROM (
          SELECT
              caea.application_id,
              caea.account_id,
              caea.current_step_id,
              caea.application_state,
              caea.application_type,
              caea.last_updated_on,
              caea.completed_on,
              caea.created_on,
              caea.platform_account_id,
              caea.partner_account_id,
              ROW_NUMBER() OVER (PARTITION BY caea.application_id ORDER BY caea.last_updated_on DESC) AS rank
          FROM
              credit_applications_entity_audit caea
          WHERE
               caea.current_step_id = 'ASSET_PLEDGE' and application_state != 'SUSPENDED'
      ) subquery
      WHERE
          subquery.rank = 1
      ) x
               left join
               (select application_id, available_portfolio_value, eligible_credit_limit, eligible_portfolio_value, pledged_credit_limit, pledged_portfolio_value, created_on
                   from credit_application_meta_data) y
               on x.application_id = y.application_id
               left join
               (select account_id, account_holderpan, account_holder_phone_number from borrower_accounts) z
               on x.account_id = z.account_id
               left join
               (select account_id, partner_name, partner_account_type from partner_accounts) pa
               on pa.account_id = x.partner_account_id
               left join
               (select platform_name, account_id from platform_accounts) as pla
               on pla.account_id= x.platform_account_id                                    
                                                   

                                                   """, conn) 
        conn.close() 

    
    return voltlms_query, customer_leads, application_data, sanctioned_amount_query, fetch_date_query, pledge_date_query


def determine_eligible_credit_limit(row):
    if row['app_eligible_credit_limit'] >= 25000:
        return row['app_eligible_credit_limit']
    elif row['mfc_eligible_credit_limit'] >= 25000:
        return row['mfc_eligible_credit_limit']
    else:
        return 0

def merging_customer_mfc_app_data(voltlms_query, customer_leads, application_data, sanctioned_amount_query):
    
    
    lpd_cl = pd.merge(customer_leads[['lead_id', 'pan']], voltlms_query[['lead_id', 'last_updated_on', 'created_on']], on='lead_id')
    lpd_cl_groupby = lpd_cl.groupby('pan').agg({'last_updated_on': 'max', 'created_on':'min'}).reset_index()
    
    # Query 02
    vlq_cl = pd.merge(left=customer_leads, right= voltlms_query, on='lead_id', how='left')
    
    vlq_cl_merged = pd.merge(left=vlq_cl, right= lpd_cl_groupby[['pan', 'last_updated_on']], on=['pan','last_updated_on'], how = 'right')

    

    application_data = application_data.reset_index(drop= True)
    df_customer_details = pd.merge(left=application_data, right=vlq_cl_merged, left_on='app_pan', right_on='pan', how='outer')

    #  Coalasce -- values  -------------------------------------------------
    df_customer_details['plaform_account_id'] = df_customer_details['platform_account_id_x'].combine_first(df_customer_details['platform_account_id_y'])
    df_customer_details['partner_account_id'] = df_customer_details['partner_account_id_x'].combine_first(df_customer_details['partner_account_id_y'])
    df_customer_details['pan'] = df_customer_details['pan'].combine_first(df_customer_details['app_pan'])
    df_customer_details['created_on'] = df_customer_details[['created_on_y', 'created_on_x']].min(axis=1)
    df_customer_details['last_updated_date'] = df_customer_details['app_last_updated_on'].combine_first(df_customer_details['last_updated_on'])
    df_customer_details['partner_name']= df_customer_details['partner_name_x'].combine_first(df_customer_details['partner_name_y'])
    df_customer_details['platform_name'] = df_customer_details['platform_name_x'].combine_first(df_customer_details['platform_name_y'])
    df_customer_details['phone_number']= df_customer_details['app_phone_number'].combine_first(df_customer_details['mfc_phone_number'])
    df_customer_details['partner_account_type']= df_customer_details['partner_account_type_x'].combine_first(df_customer_details['partner_account_type_y'])
    df_customer_details['business_channel']= df_customer_details['business_channel_x'].combine_first(df_customer_details['business_channel_y'])
    #df_customer_details['eligible_credit_limit_temp'] = df_customer_details['app_eligible_credit_limit'].combine_first(df_customer_details['mfc_eligible_credit_limit'])
    df_customer_details['eligible_credit_limit_temp'] = df_customer_details.apply(determine_eligible_credit_limit, axis=1)
    

    df_customer_details= df_customer_details.drop(columns = ['platform_account_id_x', 'platform_account_id_y', 'partner_account_id_x', 'partner_account_id_y', 
                                        'partner_name_y', 'partner_name_x', 'platform_name_y', 'platform_name_x', 'created_on_y', 'created_on_x', 'business_channel_x', 'business_channel_y'])

    # MFC Leads_Application_ID IS null() ---
    df_customer_details.loc[(df_customer_details['application_type'].isnull()) & 
        (df_customer_details['lead_id'].notnull()), 
        'application_type'] = 'CREDIT_AGAINST_SECURITIES_BORROWER'

    #  filter data for next 6 months entry:
    df_customer_details['last_updated_date'] = pd.to_datetime(df_customer_details['last_updated_date'])
    #df_customer_details= df_customer_details[df_customer_details['last_updated_date'] >= "2023-09-01"]
    # ## 
    df_customer_details = df_customer_details[df_customer_details['application_type']=='CREDIT_AGAINST_SECURITIES_BORROWER']


    df_customer_details['completed_on'] = pd.to_datetime(df_customer_details['completed_on'])
    #df_customer_details['completed_on'] = pd.to_datetime(df_customer_details['completed_on']).dt.date
    #df_customer_details['last_updated_date'] = pd.to_datetime(df_customer_details['last_updated_date']).dt.date
    #df_customer_details['created_on']= pd.to_datetime(df_customer_details['created_on']).dt.date

    

    
    # fillnan values to the columns
    columns_to_fill = ['phone_number', 'mfc_total_portfolio_value', 
                        'mfc_eligible_portfolio_value', 'mfc_eligible_credit_limit', 'app_available_portfolio_value',
                        'app_eligible_portfolio_value', 'app_eligible_credit_limit', 'app_pledged_portfolio_value']
                        
    df_customer_details[columns_to_fill] = df_customer_details[columns_to_fill].fillna(0)

    # application_stage
    df_customer_details.loc[(df_customer_details['lead_id'].notnull()) & (df_customer_details['application_id'].isnull()), 'application_stage'] = 'MFC Fetch'
    df_customer_details.loc[(df_customer_details['application_id'].notnull()), 'application_stage'] = 'Pre Fetch'
    df_customer_details.loc[(df_customer_details['app_available_portfolio_value'] > 0) & (df_customer_details['app_pledged_portfolio_value'] == 0), 'application_stage'] = 'Post Fetch'
    df_customer_details.loc[(df_customer_details['app_pledged_portfolio_value'] > 0) & (df_customer_details['application_state'] == 'IN_PROGRESS'), 'application_stage'] = 'Post Pledge'
    df_customer_details.loc[(df_customer_details['application_state'] == 'COMPLETED'), 'application_stage'] = 'Completed'

    app_conditions = [
        df_customer_details['lead_id'].notnull(), 
        df_customer_details['lead_id'].isnull()]

    # mfc & app journey's
    choices = ['mfc_journey', 'app_only_journey' ]
    df_customer_details['app_journey'] = np.select(app_conditions, choices, default='Other')

    df_customer_details= df_customer_details[['platform_name', 'partner_name', 'partner_account_type', 'pan', 'phone_number', 'mfc_total_portfolio_value', 'mfc_eligible_portfolio_value', 
                    'mfc_eligible_credit_limit', 'app_available_portfolio_value', 'app_eligible_portfolio_value', 'app_eligible_credit_limit', 
                    'app_pledged_portfolio_value', 'app_pledged_credit_limit','app_current_step_id', 'last_updated_date', 'created_on', 'completed_on', 'business_channel', 
                    'application_stage', 'app_journey', 'application_type', 'application_state', 'lead_id', 'application_id', 'eligible_credit_limit_temp']]

    print ("DB Checked")
    
    df_customer_details['phone_number'] = df_customer_details['phone_number'].apply(vpn.validate_phone)
    df_customer_details = df_customer_details[['pan','platform_name','partner_name','partner_account_type','phone_number','mfc_total_portfolio_value', 'mfc_eligible_portfolio_value', 
                    'mfc_eligible_credit_limit', 'app_available_portfolio_value', 'app_eligible_portfolio_value', 'app_eligible_credit_limit','app_current_step_id',
                    'last_updated_date','created_on','completed_on','business_channel','application_stage','application_type','application_state','application_id','eligible_credit_limit_temp']]
    
    df_customer_details['completed_on'] = df_customer_details.apply(lambda row: row['last_updated_date'] if row['application_stage'] == 'Completed' and pd.isna(row['completed_on']) else row['completed_on'], axis=1)
    #df_customer_details['eligible'] = df_customer_details.apply(lambda x: 'Yes' if x['mfc_eligible_credit_limit'] >= 25000 or x['app_eligible_credit_limit'] >= 25000 else 'No', axis=1)
    df_customer_details['eligible'] = df_customer_details.apply(lambda x: 'Yes' if x['eligible_credit_limit_temp'] >= 25000  else 'No', axis=1)
    
    df_customer_details = df_customer_details[df_customer_details['eligible'] == 'Yes']


    df_customer_details['platform_type']= ''
    
    df_customer_details['platform_type'] = df_customer_details['platform_name'].apply(determine_platform_type)
    
    df_customer_details = pd.merge(df_customer_details, sanctioned_amount_query, on = 'application_id', how= 'left')
    
    return df_customer_details

def determine_platform_type(platform_name):
    if platform_name in ['Volt Website', 'Volt Mobile App', 'Volt Partner Android App']:
        return 'MFD Direct'
    elif platform_name in ['Redvision Technologies', 'Investwell SDK', 'Z_FUNDS', 'Asset Plus', 'Advisor khoj']:
        return 'MFD Software'
    elif platform_name in ['JUPITER', 'BharatNxt', 'BharatNxt1', 'Indifi', 'PhonePe', 'Park plus', 'SANKASH', 'Beyond IRR', 'MERCURY', 'Freo']:
        return 'B2B Partners'
    else:
        return 'Other'

def filter_and_group_platforms_completed(df, period):
    # Convert 'completed_on' to datetime if it is not already
    df['completed_on'] = pd.to_datetime(df['completed_on'])
    
    if period == 'D':
        last_15_days = pd.Timestamp.now() - pd.Timedelta(days=15)
        df = df[df['completed_on'] >= last_15_days]
    
    # Filter the dataframe
    filtered_df = df[ 
                     (df['app_current_step_id'].isnull()) & 
                     (df['application_state'] == 'COMPLETED')]

    # Extract month and year from 'completed_on'
    filtered_df['completed_month'] = filtered_df['completed_on'].dt.to_period(period)

    # Group by 'completed_month' and 'platform_name'
    grouped = filtered_df.groupby(['completed_month', 'platform_name']).agg(
        count=('platform_name', 'size'),
        approved_credit_count=('sanctioned_limit', 'sum'),
        average_eligible_credit=('eligible_credit_limit_temp', 'mean'),
        median_eligible_credit=('eligible_credit_limit_temp', 'median')
    ).reset_index()
    
    return grouped

def filter_and_group_platform_type_completed(df, period):
    # Convert 'completed_on' to datetime if it is not already
    df['completed_on'] = pd.to_datetime(df['completed_on'])
    
    if period == 'D':
        last_15_days = pd.Timestamp.now() - pd.Timedelta(days=15)
        df = df[df['completed_on'] >= last_15_days]
    
    # Filter the dataframe
    filtered_df = df[ 
                     (df['app_current_step_id'].isnull()) & 
                     (df['application_state'] == 'COMPLETED')]

    # Extract month and year from 'completed_on'
    filtered_df['completed_month'] = filtered_df['completed_on'].dt.to_period(period)

    # Group by 'completed_month' and 'platform_name'
    grouped = filtered_df.groupby(['completed_month', 'platform_type']).agg(
        count=('platform_type', 'size'),
        approved_credit_count=('sanctioned_limit', 'sum'),
        average_eligible_credit=('eligible_credit_limit_temp', 'mean'),
        median_eligible_credit=('eligible_credit_limit_temp', 'median')
    ).reset_index()
    
    grouped.rename(columns = {'platform_type':'platform_name'}, inplace = True)
    
    return grouped

def filter_and_group_platforms_registered(df, period):
    # Convert 'created_on' to datetime if it is not already
    df['created_on'] = pd.to_datetime(df['created_on'])
    
    # Extract month and year from 'created_on'
    df['created_month'] = df['created_on'].dt.to_period(period)

    # Group by 'created_month' and 'platform_name'
    grouped = df.groupby(['created_month', 'platform_name']).agg(
        count=('platform_name', 'size'),
        approved_credit_count=('sanctioned_limit', 'sum'),
        average_eligible_credit=('eligible_credit_limit_temp', 'mean'),
        median_eligible_credit=('eligible_credit_limit_temp', 'median')
    ).reset_index()
    
    return grouped

def filter_and_group_platform_type_registered(df, period):
    # Convert 'created_on' to datetime if it is not already
    df['created_on'] = pd.to_datetime(df['created_on'])
    
    # Extract month and year from 'created_on'
    df['created_month'] = df['created_on'].dt.to_period(period)

    # Group by 'created_month' and 'platform_name'
    grouped = df.groupby(['created_month', 'platform_type']).agg(
        count=('platform_type', 'size'),
        approved_credit_count=('sanctioned_limit', 'sum'),
        average_eligible_credit=('eligible_credit_limit_temp', 'mean'),
        median_eligible_credit=('eligible_credit_limit_temp', 'median')
    ).reset_index()
    
    grouped.rename(columns = {'platform_type':'platform_name'}, inplace = True)
    
    return grouped


def filter_and_group_platform_type_all(df, date_column, type, period):
    # Convert 'completed_on' to datetime if it is not already
    if date_column in df.columns:
        df[date_column] = pd.to_datetime(df[date_column])
    
    # Conditional filtering based on the presence of 'completed_on'
    if date_column == 'completed_on':
        filtered_df = df[
            (df['app_current_step_id'].isnull()) & 
            (df['application_state'] == 'COMPLETED')
        ]
    else:
        filtered_df = df[
            (df['app_current_step_id'].isnull()) & 
            (df['application_state'] == 'COMPLETED')
        ]
    
    # Extract the period from 'completed_on' if available
    if date_column in filtered_df.columns:
        filtered_df['completed_period'] = filtered_df[date_column].dt.to_period(period)

    # Group by 'completed_period' and 'platform_type'
    grouped = filtered_df.groupby(['completed_period', type]).agg(
        count=(type, 'size'),
        approved_credit_count=('sanctioned_limit', lambda x: x.notnull().sum()),
        average_eligible_credit=('eligible_credit_limit_temp', 'mean'),
        median_eligible_credit=('eligible_credit_limit_temp', 'median'),
        sum_eligible_credit=('eligible_credit_limit_temp', 'sum')
    ).reset_index()

    grouped.rename(columns={'platform_type': 'platform_name'}, inplace=True)
    
    return grouped

if __name__ == "__main__":
    main()