#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 26 13:04:34 2024

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

import global_parameters as g




def main():
    #final_df_aggregate = get_db_withdrawal_data()
    withdrawal_data, collections_data, lien_removal_data, foreclosure_data, account_opening_data, lodgement_data, enhancement_request_data, final_df_aggregate = get_db_withdrawal_data()

    update_worksheet(g.secret_file, g.mis_withdrawal_sheet_id, "Data Withdrawal", withdrawal_data)
    update_worksheet(g.secret_file, g.mis_withdrawal_sheet_id, "Data Repayments", collections_data)
    update_worksheet(g.secret_file, g.mis_withdrawal_sheet_id, "Data Lien Removal", lien_removal_data)
    update_worksheet(g.secret_file, g.mis_withdrawal_sheet_id, "Data Foreclosure", foreclosure_data)
    update_worksheet(g.secret_file, g.mis_withdrawal_sheet_id, "Data Account Opening", account_opening_data)
    update_worksheet(g.secret_file, g.mis_withdrawal_sheet_id, "Data Lodgement", lodgement_data)
    update_worksheet(g.secret_file, g.mis_withdrawal_sheet_id, "Data Enhancement", enhancement_request_data)
    update_worksheet(g.secret_file, g.mis_withdrawal_sheet_id, "Aggregate sheet Overall Data", final_df_aggregate)



def update_worksheet(service_account_file, spreadsheet_id, title, dataframe):
    client = pygsheets.authorize(service_account_file=service_account_file)
    sh = client.open_by_key(spreadsheet_id)
    ws = sh.worksheet("title", title)
    ws.resize(dataframe.shape[0], dataframe.shape[1])
    ws.clear(start='A1', end=None)
    ws.set_dataframe(dataframe, start=(1, 1))
    
def update_worksheet22(service_account_file, spreadsheet_id, title, dataframe, start):
    client = pygsheets.authorize(service_account_file=service_account_file)
    sh = client.open_by_key(spreadsheet_id)
    ws = sh.worksheet("title", title)
    ws.resize(dataframe.shape[0], dataframe.shape[1])
    ws.clear(start=start, end=None)
    ws.set_dataframe(dataframe, start=(1, 1))
    
def update_worksheet2(service_account_file, spreadsheet_id, title, dataframe):
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

def get_db_withdrawal_data():
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

        withdrawal_data= pd.read_sql_query("""
          
      select requested_date, settled_under_time, time_delay_till_today, delay_in_settlement,count(1) from (

   SELECT 
    DATE(requested_on) AS requested_date,
    disbursal_id,
    disbursal_status,
    CASE 
        WHEN EXTRACT(EPOCH FROM (last_updated_on - requested_on)) / 3600 <= 2 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (last_updated_on - requested_on)) / 3600 <= 6 THEN 'L2'
        
        ELSE 'L3'
    END AS delay_hour,
    CASE 
        WHEN EXTRACT(EPOCH FROM (expected_transfer_by - requested_on)) / 3600 <= 2 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (expected_transfer_by - requested_on)) / 3600 <= 6 THEN 'L2'
        
        ELSE 'L3'
    END AS expected_transfer_bucket,
    CASE 
        WHEN disbursal_status = 'SETTLED' AND 
             EXTRACT(EPOCH FROM (last_updated_on - requested_on)) / 3600 > EXTRACT(EPOCH FROM (expected_transfer_by - requested_on)) / 3600 THEN 'NO'
        when disbursal_status not in ('SETTLED','REJECTED','FAILED') and
        	 EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - requested_on)) / 3600 > EXTRACT(EPOCH FROM (expected_transfer_by - requested_on)) / 3600 
			 then 'NOT SETTLED SLA BREACHED'
		when disbursal_status not in ('SETTLED','REJECTED','FAILED') and
        	 EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - requested_on)) / 3600 <= EXTRACT(EPOCH FROM (expected_transfer_by - requested_on)) / 3600 
			 then 'NOT SETTLED NO SLA BREACHED'
        when disbursal_status in ('REJECTED','FAILED') then 'FAILED'
        ELSE 'YES'
    END AS settled_under_time,
    CASE 
        WHEN disbursal_status = 'SETTLED' THEN NULL
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 3600 <= 2 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 3600 <= 6 THEN 'L2'
        
        ELSE 'L3'
    END AS time_delay_till_today,
    CASE 
        WHEN  disbursal_status = 'SETTLED' AND 
             EXTRACT(EPOCH FROM (last_updated_on - requested_on)) / 3600 > EXTRACT(EPOCH FROM (expected_transfer_by - requested_on)) / 3600 THEN
            CASE 
                WHEN EXTRACT(EPOCH FROM (last_updated_on - expected_transfer_by)) / 3600 <= 2 THEN 'L1'
                WHEN EXTRACT(EPOCH FROM (last_updated_on - expected_transfer_by)) / 3600 <= 6 THEN 'L2'
                
                ELSE 'L3'
            END
        ELSE NULL
    END AS delay_in_settlement,
    CASE 
        WHEN  disbursal_status != 'SETTLED' AND 
             EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - requested_on)) / 3600 > EXTRACT(EPOCH FROM (expected_transfer_by - requested_on)) / 3600 THEN
            CASE 
                WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - expected_transfer_by)) / 3600 <= 2 THEN 'L1'
                WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - expected_transfer_by)) / 3600 <= 6 THEN 'L2'
                
                ELSE 'L3'
            END
        ELSE NULL
    END AS delay_in_not_settlement,
    requested_on, last_updated_on,expected_transfer_by
FROM 
    DISBURSALS
    where date(requested_on) >= CURRENT_DATE - INTERVAL '40 days') as boo
    group by requested_date, settled_under_time, time_delay_till_today, delay_in_settlement

   


                                """, conn) 
        conn.close() 
    
    withdrawal_data.replace(to_replace=pd.NA, value='', inplace=True)
    
    
    
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

        collections_data= pd.read_sql_query("""
             
      select requested_date, SUCCESS_under_time, time_delay_till_today, delay_in_settlement,count(1) from (

   SELECT 
    DATE(created_on) AS requested_date,
    collection_id,
    collection_status,
    CASE 
        WHEN EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 <= 2 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 <= 6 THEN 'L2'
        
        ELSE 'L3'
    END AS delay_hour,
    CASE 
        WHEN EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 <= 2 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 <= 6 THEN 'L2'
        
        ELSE 'L3'
    END AS expected_transfer_bucket,
    CASE 
        WHEN collection_status = 'SETTLED' AND 
             EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 THEN 'NO'
        when collection_status not in ('REQUESTED','SETTLED','CANCELLED','FAILED') and
        	 EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 
			 then 'NOT SETTLED SLA BREACHED'
		when collection_status not in ('REQUESTED','SETTLED','CANCELLED','FAILED') and
        	 EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_on)) / 3600 <= EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 
			 then 'NOT SETTLED NO SLA BREACHED'
        when collection_status in ('CANCELLED','FAILED') then 'FAILED'
        when collection_status in ('REQUESTED') then 'REQUESTED'
        ELSE 'YES'
    END AS SUCCESS_under_time,
    CASE 
        WHEN collection_status = 'SETTLED' THEN NULL
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 3600 <= 2 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 3600 <= 6 THEN 'L2'
        
        ELSE 'L3'
    END AS time_delay_till_today,
    CASE 
        WHEN  collection_status = 'SETTLED' AND 
             EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 THEN
            CASE 
                WHEN EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 3600 <= 2 THEN 'L1'
                WHEN EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 3600 <= 6 THEN 'L2'
                
                ELSE 'L3'
            END
        ELSE NULL
    END AS delay_in_settlement,
    CASE 
        WHEN  collection_status != 'SETTLED' AND 
             EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 THEN
            CASE 
                WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_expected_by)) / 3600 <= 2 THEN 'L1'
                WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_expected_by)) / 3600 <= 6 THEN 'L2'
                
                ELSE 'L3'
            END
        ELSE NULL
    END AS delay_in_not_settlement,
    created_on, last_updated_on,settlement_expected_by
FROM 
    collections
    where date(created_on) >= CURRENT_DATE - INTERVAL '40 days') as boo
    group by requested_date, SUCCESS_under_time, time_delay_till_today, delay_in_settlement

                                """, conn) 
        conn.close() 
    
    collections_data.replace(to_replace=pd.NA, value='', inplace=True)
    
    
    
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

        lien_removal_data= pd.read_sql_query("""
             
      select requested_date, SUCCESS_under_time, time_delay_till_today, delay_in_settlement,count(1) from (

   SELECT 
    DATE(created_on) AS requested_date,
    request_id,
    status,
    CASE 
        WHEN EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 86400 <= 1 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 86400 <= 3 THEN 'L2'
        
        ELSE 'L3'
    END AS delay_hour,
    CASE 
        WHEN EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 86400 <= 1 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 86400 <= 3 THEN 'L2'
        
        ELSE 'L3'
    END AS expected_transfer_bucket,
    CASE 
        WHEN status = 'SUCCESS' AND 
             EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 THEN 'NO'
        when status not in ('SUCCESS','FAILED') and
             EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 
             then 'NOT SETTLED SLA BREACHED'
        when status not in ('SUCCESS','FAILED') and
             EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_on)) / 3600 <= EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 
             then 'NOT SETTLED NO SLA BREACHED'
        when status in ('FAILED') then 'FAILED'
        --when status in ('REQUESTED') then 'REQUESTED'
        ELSE 'YES'
    END AS SUCCESS_under_time,
    CASE 
        WHEN status = 'SUCCESS' THEN NULL
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 1 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 3 THEN 'L2'
        
        ELSE 'L3'
    END AS time_delay_till_today,
    CASE 
        WHEN  status = 'SUCCESS' AND 
             EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 THEN
            CASE 
                WHEN EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 <= 1 THEN 'L1'
                WHEN EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 <= 3 THEN 'L2'
                
                ELSE 'L3'
            END
        ELSE NULL
    END AS delay_in_settlement,
    CASE 
        WHEN  status != 'SUCCESS' AND 
             EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 THEN
            CASE 
                WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_expected_by)) / 86400 <= 1 THEN 'L1'
                WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_expected_by)) / 86400 <= 3 THEN 'L2'
                
                ELSE 'L3'
            END
        ELSE NULL
    END AS delay_in_not_settlement,
    created_on, last_updated_on,settlement_expected_by
FROM 
    (select rr.*, cm.lending_partner_id,
  case when cm.lending_partner_id = 'Bajaj' then rr.created_on + INTERVAL '7 days'
   when cm.lending_partner_id = 'Tata' then rr.created_on + INTERVAL '5 days'
  else null 
  end as settlement_expected_by
  from revocation_requests rr left join 
  credit_main cm on rr.credit_id = cm.credit_id  ) as foo
    where date(created_on) >= CURRENT_DATE - INTERVAL '40 days') as boo
    group by requested_date, SUCCESS_under_time, time_delay_till_today, delay_in_settlement

                                """, conn) 
        conn.close() 
    
    lien_removal_data.replace(to_replace=pd.NA, value='', inplace=True)
    
    
    
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

        foreclosure_data= pd.read_sql_query("""
             
      select requested_date, SUCCESS_under_time, time_delay_till_today, delay_in_settlement,count(1) from (

   SELECT 
    DATE(created_on) AS requested_date,
    request_id,
    status,
    CASE 
        WHEN EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 86400 <= 1 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 86400 <= 3 THEN 'L2'
        
        ELSE 'L3'
    END AS delay_hour,
    CASE 
        WHEN EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 86400 <= 1 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 86400 <= 3 THEN 'L2'
        
        ELSE 'L3'
    END AS expected_transfer_bucket,
    CASE 
        WHEN status = 'SUCCESS' AND 
             EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 THEN 'NO'
        when status not in ('SUCCESS','FAILED') and
             EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 
             then 'NOT SETTLED SLA BREACHED'
        when status not in ('SUCCESS','FAILED') and
             EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_on)) / 3600 <= EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 
             then 'NOT SETTLED NO SLA BREACHED'
        when status in ('FAILED') then 'FAILED'
        --when status in ('REQUESTED') then 'REQUESTED'
        ELSE 'YES'
    END AS SUCCESS_under_time,
    CASE 
        WHEN status = 'SUCCESS' THEN NULL
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 1 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 3 THEN 'L2'
        
        ELSE 'L3'
    END AS time_delay_till_today,
    CASE 
        WHEN  status = 'SUCCESS' AND 
             EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 THEN
            CASE 
                WHEN EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 <= 1 THEN 'L1'
                WHEN EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 <= 3 THEN 'L2'
                
                ELSE 'L3'
            END
        ELSE NULL
    END AS delay_in_settlement,
    CASE 
        WHEN  status != 'SUCCESS' AND 
             EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 THEN
            CASE 
                WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_expected_by)) / 86400 <= 1 THEN 'L1'
                WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_expected_by)) / 86400 <= 3 THEN 'L2'
                
                ELSE 'L3'
            END
        ELSE NULL
    END AS delay_in_not_settlement,
    created_on, last_updated_on,settlement_expected_by
FROM 
    (SELECT *, 
       created_on + INTERVAL '5 days' AS settlement_expected_by
FROM foreclosure_requests where lender_id = 'Bajaj'
union
SELECT *, 
       created_on + INTERVAL '7 days' AS settlement_expected_by
FROM foreclosure_requests where lender_id = 'Tata' ) as foo
    where date(created_on) >= CURRENT_DATE - INTERVAL '40 days') as boo
    group by requested_date, SUCCESS_under_time, time_delay_till_today, delay_in_settlement

                                """, conn) 
        conn.close() 
    
    foreclosure_data.replace(to_replace=pd.NA, value='', inplace=True)
    
    
    
    
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

        account_opening_data= pd.read_sql_query("""
                                                
        select credit_created_on_date, settled_under_time, time_delay_till_today, delay_in_settlement,count(1) from (
                                           
    select 
    DATE(credit_created_on) AS credit_created_on_date,
    credit_id,
    credit_status,
    CASE 
        WHEN EXTRACT(EPOCH FROM (account_opening_time_actual - credit_created_on)) / 60 <= 120 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (account_opening_time_actual - credit_created_on)) / 60 <= 3600 THEN 'L2'
        
        ELSE 'L3'
    END AS delay_hour,
    CASE 
        WHEN EXTRACT(EPOCH FROM (settlement_account_opening_time - credit_created_on)) / 60 <= 120 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (settlement_account_opening_time - credit_created_on)) / 60 <= 3600 THEN 'L2'
        
        ELSE 'L3'
    END AS expected_transfer_bucket,
    
    CASE 
        WHEN settlement_account_opening_time >= account_opening_time_actual THEN 'YES'
        when credit_status = 'ACTIVE' and account_opening_time_actual is null then 'YES'
        WHEN account_opening_time_actual > settlement_account_opening_time THEN 'NO'
        WHEN account_opening_time_actual is null and settlement_account_opening_time > CURRENT_TIMESTAMP THEN 'NOT SETTLED NO SLA BREACHED'
        WHEN account_opening_time_actual is null and CURRENT_TIMESTAMP > settlement_account_opening_time THEN 'NOT SETTLED SLA BREACHED'
		ELSE 'DONT KNOW'
    END AS settled_under_time,
    -----
    CASE 
        WHEN credit_status != 'PENDING_DISBURSAL_APPROVAL' THEN NULL
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - account_opening_time_actual)) / 60 <= 120 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - account_opening_time_actual)) / 60 <= 3600 THEN 'L2'
        
        ELSE 'L3'
    END AS time_delay_till_today,
    CASE 
        WHEN  credit_status not in ('PENDING_DISBURSAL_APPROVAL') AND 
             EXTRACT(EPOCH FROM (account_opening_time_actual - credit_created_on)) / 60 > EXTRACT(EPOCH FROM (settlement_account_opening_time - credit_created_on)) / 60 THEN
            CASE 
                WHEN EXTRACT(EPOCH FROM (account_opening_time_actual - settlement_account_opening_time)) / 60 <= 120 THEN 'L1'
                WHEN EXTRACT(EPOCH FROM (account_opening_time_actual - settlement_account_opening_time)) / 60 <= 3600 THEN 'L2'
                
                ELSE 'L3'
            END
        ELSE NULL
    END AS delay_in_settlement,
    CASE 
        WHEN  credit_status = 'PENDING_DISBURSAL_APPROVAL' AND 
             EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - credit_created_on)) / 60 > EXTRACT(EPOCH FROM (settlement_account_opening_time - credit_created_on)) / 60 THEN
            CASE 
                WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_account_opening_time)) / 60 <= 120 THEN 'L1'
                WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_account_opening_time)) / 60 <= 3600 THEN 'L2'
                
                ELSE 'L3'
            END
        ELSE NULL
    END AS delay_in_not_settlement,credit_created_on,account_opening_time_actual,settlement_account_opening_time
    
    from
(select 
        cm.credit_id, cm.credit_status, 
        cm.lending_partner_id, 
    cm.created_on as credit_created_on, account_opening_time_actual, 
    --(account_opening_time - created_on) as account_opening_turnaround_time, 
    
    case when cm.lending_partner_id = 'Tata' then created_on + INTERVAL '30 minutes' 
 		when cm.lending_partner_id = 'Bajaj' then created_on + INTERVAL '4 hours' 
 		ELSE NULL 
 		END AS settlement_account_opening_time,
 		lodgement_time_actual,
 		--(lodgement_time- account_opening_time) as lodgement_turnaround_time, 
 	case when cm.lending_partner_id = 'Tata' then lodgement_time_actual + INTERVAL '15 minutes' 
 		when cm.lending_partner_id = 'Bajaj' then lodgement_time_actual + INTERVAL '2 hours' 
 		ELSE NULL 
 		END AS settlement_lodgement_time,	
 
    cm.account_id 
from credit_main cm
left join 
(select credit_id, min(last_updated_on) as account_opening_time_actual
    from credit_audit
    where credit_status in ('PENDING_LODGEMENT', 'APPROVED_NOT_DISBURSED','ACTIVE','PENDING_CLOSURE')
    group by credit_id) pl 
    on cm.credit_id = pl.credit_id
left join 
(select credit_id, min(last_updated_on) as lodgement_time_actual
    from credit_audit
    where credit_status IN ('APPROVED_NOT_DISBURSED','ACTIVE','PENDING_CLOSURE')
    group by credit_id) an 
    on cm.credit_id = an.credit_id
left join
(select account_holder_name, account_holderpan, account_id from borrower_accounts) ba
on ba.account_id= cm.account_id) credit_data
where date(credit_created_on) >= CURRENT_DATE - INTERVAL '40 days') as boo
    group by credit_created_on_date, settled_under_time, time_delay_till_today, delay_in_settlement

                 """, conn) 
        conn.close() 
    
    account_opening_data.replace(to_replace=pd.NA, value='', inplace=True)
    
    
    
    
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

        lodgement_data= pd.read_sql_query("""
                                                
        select credit_created_on, settled_under_time, time_delay_till_today, delay_in_settlement,count(1) from (
                                     
    select 
    DATE(credit_created_on) AS credit_created_on,
    credit_id,
    credit_status,
    CASE 
        WHEN EXTRACT(EPOCH FROM (lodgement_time_actual - account_opening_time_actual)) / 60 <= 120 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (lodgement_time_actual - account_opening_time_actual)) / 60 <= 3600 THEN 'L2'
        
        ELSE 'L3'
    END AS delay_hour,
    CASE 
        WHEN EXTRACT(EPOCH FROM (settlement_lodgement_time - account_opening_time_actual)) / 60 <= 120 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (settlement_lodgement_time - account_opening_time_actual)) / 60 <= 3600 THEN 'L2'
        
        ELSE 'L3'
    END AS expected_transfer_bucket,
    
    CASE 
        WHEN settlement_lodgement_time >= lodgement_time_actual THEN 'YES'
        when credit_status IN ('ACTIVE','PENDING_CLOSURE','CLOSED') and lodgement_time_actual is null then 'YES'
        WHEN lodgement_time_actual > settlement_lodgement_time THEN 'NO'
        WHEN lodgement_time_actual is null and settlement_lodgement_time > CURRENT_TIMESTAMP and credit_status IN ('PENDING_DISBURSAL_APPROVAL','PENDING_LODGEMENT') THEN 'NOT SETTLED NO SLA BREACHED'
        WHEN lodgement_time_actual is null and CURRENT_TIMESTAMP > settlement_lodgement_time THEN 'NOT SETTLED SLA BREACHED'
		ELSE 'DONT KNOW'
    END AS settled_under_time,
    -----
    CASE 
        WHEN credit_status not in ('PENDING_DISBURSAL_APPROVAL','PENDING_LODGEMENT') THEN NULL
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - lodgement_time_actual)) / 60 <= 120 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - lodgement_time_actual)) / 60 <= 3600 THEN 'L2'
        
        ELSE 'L3'
    END AS time_delay_till_today,
    CASE 
        WHEN  credit_status not in ('PENDING_DISBURSAL_APPROVAL','PENDING_LODGEMENT', 'APPROVED_NOT_DISBURSED') AND 
             EXTRACT(EPOCH FROM (lodgement_time_actual - account_opening_time_actual)) / 60 > EXTRACT(EPOCH FROM (settlement_lodgement_time - account_opening_time_actual)) / 60 THEN
            CASE 
                WHEN EXTRACT(EPOCH FROM (lodgement_time_actual - settlement_lodgement_time)) / 60 <= 120 THEN 'L1'
                WHEN EXTRACT(EPOCH FROM (lodgement_time_actual - settlement_lodgement_time)) / 60 <= 3600 THEN 'L2'
                
                ELSE 'L3'
            END
        ELSE NULL
    END AS delay_in_settlement,
    CASE 
        WHEN  credit_status = 'PENDING_DISBURSAL_APPROVAL' AND 
             EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - account_opening_time_actual)) / 60 > EXTRACT(EPOCH FROM (settlement_lodgement_time - credit_created_on)) / 60 THEN
            CASE 
                WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_lodgement_time)) / 60 <= 120 THEN 'L1'
                WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_lodgement_time)) / 60 <= 3600 THEN 'L2'
                
                ELSE 'L3'
            END
        ELSE NULL
    END AS delay_in_not_settlement,account_opening_time_actual,lodgement_time_actual,settlement_lodgement_time
    
    from
(select 
        cm.credit_id, cm.credit_status, 
        cm.lending_partner_id, 
    cm.created_on as credit_created_on, account_opening_time_actual, 
    --(account_opening_time - created_on) as account_opening_turnaround_time, 
    
    case when cm.lending_partner_id = 'Tata' then created_on + INTERVAL '15 minutes' 
 		when cm.lending_partner_id = 'Bajaj' then created_on + INTERVAL '2 hours' 
 		ELSE NULL 
 		END AS settlement_account_opening_time,
 		lodgement_time_actual,
 		--(lodgement_time- account_opening_time) as lodgement_turnaround_time, 
 	case when cm.lending_partner_id = 'Tata'  and lodgement_time_actual is not null then lodgement_time_actual + INTERVAL '15 minutes' 
 		when cm.lending_partner_id = 'Bajaj' and lodgement_time_actual is not null then lodgement_time_actual + INTERVAL '2 hours' 
         when cm.lending_partner_id = 'Tata' and lodgement_time_actual is null and account_opening_time_actual is not null then account_opening_time_actual + INTERVAL '30 minutes' 
     	when cm.lending_partner_id = 'Bajaj' and lodgement_time_actual is null and account_opening_time_actual is not null then account_opening_time_actual + INTERVAL '4 hours' 
         when cm.lending_partner_id = 'Tata' and lodgement_time_actual is null and account_opening_time_actual is  null then cm.created_on + INTERVAL '30 minutes' 
     	when cm.lending_partner_id = 'Bajaj' and lodgement_time_actual is null and account_opening_time_actual is  null then cm.created_on + INTERVAL '4 hours' 
 		ELSE NULL 
 		END AS settlement_lodgement_time,	
 
    cm.account_id 
from credit_main cm
left join 
(select credit_id, min(last_updated_on) as account_opening_time_actual
    from credit_audit
    where credit_status in ('PENDING_LODGEMENT', 'APPROVED_NOT_DISBURSED','ACTIVE','PENDING_CLOSURE')
    group by credit_id) pl 
    on cm.credit_id = pl.credit_id
left join 
(select credit_id, min(last_updated_on) as lodgement_time_actual
    from credit_audit
    where credit_status IN ('APPROVED_NOT_DISBURSED','ACTIVE','PENDING_CLOSURE')
    group by credit_id) an 
    on cm.credit_id = an.credit_id
left join
(select account_holder_name, account_holderpan, account_id from borrower_accounts) ba
on ba.account_id= cm.account_id) credit_data
where date(credit_created_on) >= CURRENT_DATE - INTERVAL '40 days') as boo
    group by credit_created_on, settled_under_time, time_delay_till_today, delay_in_settlement



                 """, conn) 
        conn.close() 
    
    lodgement_data.replace(to_replace=pd.NA, value='', inplace=True)
    
    
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

        enhancement_request_data= pd.read_sql_query("""
          
       select created_on_date, settled_under_time, time_delay_till_today, delay_in_settlement,count(1) from (
                                                
   SELECT 
    DATE(created_on) AS created_on_date,
    request_id,
    status,
    CASE 
        WHEN EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 <= 2 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 <= 6 THEN 'L2'
        
        ELSE 'L3'
    END AS delay_hour,
    CASE 
        WHEN EXTRACT(EPOCH FROM (expected_transfer_by - created_on)) / 3600 <= 2 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (expected_transfer_by - created_on)) / 3600 <= 6 THEN 'L2'
        
        ELSE 'L3'
    END AS expected_transfer_bucket,
    CASE 
        WHEN status = 'SUCCESS' AND 
             EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (expected_transfer_by - created_on)) / 3600 THEN 'NO'
        when status = 'REQUESTED' and
        	 EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_on)) / 3600 > EXTRACT(EPOCH FROM (expected_transfer_by - created_on)) / 3600 
			 then 'NOT SETTLED SLA BREACHED'
		when status = 'REQUESTED' and
        	 EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_on)) / 3600 <= EXTRACT(EPOCH FROM (expected_transfer_by - created_on)) / 3600 
			 then 'NOT SETTLED NO SLA BREACHED'
        when status = 'FAILED' then 'FAILED'
        ELSE 'YES'
    END AS settled_under_time,
    CASE 
        WHEN status = 'SUCCESS' THEN NULL
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 3600 <= 2 THEN 'L1'
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 3600 <= 6 THEN 'L2'
        
        ELSE 'L3'
    END AS time_delay_till_today,
    CASE 
        WHEN  status = 'SUCCESS' AND 
             EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (expected_transfer_by - created_on)) / 3600 THEN
            CASE 
                WHEN EXTRACT(EPOCH FROM (last_updated_on - expected_transfer_by)) / 3600 <= 2 THEN 'L1'
                WHEN EXTRACT(EPOCH FROM (last_updated_on - expected_transfer_by)) / 3600 <= 6 THEN 'L2'
                
                ELSE 'L3'
            END
        ELSE NULL
    END AS delay_in_settlement,
    CASE 
        WHEN  status = 'REQUESTED' AND 
             EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_on)) / 3600 > EXTRACT(EPOCH FROM (expected_transfer_by - created_on)) / 3600 THEN
            CASE 
                WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - expected_transfer_by)) / 3600 <= 2 THEN 'L1'
                WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - expected_transfer_by)) / 3600 <= 6 THEN 'L2'
                
                ELSE 'L3'
            END
        ELSE NULL
    END AS delay_in_not_settlement,
    created_on, last_updated_on,expected_transfer_by
FROM 
    (   select request_id ,created_on , status ,last_updated_on , created_on + INTERVAL '2 hours' as expected_transfer_by from enhance_limit_request elr where date(created_on) >= CURRENT_DATE - INTERVAL '40 days') as foo
   ) as boo group by created_on_date, settled_under_time, time_delay_till_today, delay_in_settlement

   


                                """, conn) 
        conn.close() 
    
    enhancement_request_data.replace(to_replace=pd.NA, value='', inplace=True)
    
    
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

        customer_success_lms= pd.read_sql_query("""
          
      
   
   
  ( WITH success_query AS (
    SELECT 
        DATE_TRUNC('day', last_updated_on) AS update_date,lending_partner_id,
        COUNT(CASE 
            WHEN status = 'SUCCESS' THEN 1 
            ELSE NULL 
        END) AS total_success_today,
        COUNT(CASE 
            WHEN status = 'SUCCESS' AND last_updated_on > settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS sla_breached,
        COUNT(CASE 
            WHEN status = 'SUCCESS' AND last_updated_on <= settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS within_sla,
        COUNT(CASE 
            WHEN status = 'SUCCESS' AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 3600 <= 2 THEN 1 
            ELSE NULL 
        END) AS L1,
        COUNT(CASE 
            WHEN status = 'SUCCESS' AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 3600 > 2 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 3600 <= 6 THEN 1 
            ELSE NULL 
        END) AS L2,
        COUNT(CASE 
            WHEN status = 'SUCCESS' AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 3600 > 6 THEN 1 
            ELSE NULL 
        END) AS L3
    FROM (
        select elr.created_on , status, cm.lending_partner_id ,elr.last_updated_on , elr.created_on + INTERVAL '2 hours' as settlement_expected_by 
from enhance_limit_request elr left join credits cm on elr.credit_id = cm.credit_id
    ) AS subquery
    WHERE DATE_TRUNC('day', last_updated_on) = CURRENT_DATE
    GROUP BY DATE_TRUNC('day', last_updated_on),lending_partner_id
),
non_success_query AS (
    SELECT 
        DATE_TRUNC('day', current_timestamp) AS update_date,lending_partner_id,
        COUNT(CASE 
            WHEN status != 'SUCCESS' THEN 1 
            ELSE NULL 
        END) AS total_non_success,
        COUNT(CASE 
            WHEN status != 'SUCCESS' AND current_timestamp > settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS sla_breached,
        COUNT(CASE 
            WHEN status != 'SUCCESS' AND current_timestamp <= settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS within_sla,
        COUNT(CASE 
            WHEN status != 'SUCCESS' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 1 THEN 1 
            ELSE NULL 
        END) AS L1,
        COUNT(CASE 
            WHEN status != 'SUCCESS' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 > 1 AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 3 THEN 1 
            ELSE NULL 
        END) AS L2,
        COUNT(CASE 
            WHEN status != 'SUCCESS' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 > 3 AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 5 THEN 1 
            ELSE NULL 
        END) AS L3,
        COUNT(CASE 
            WHEN status != 'SUCCESS' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 > 5 THEN 1 
            ELSE NULL 
        END) AS L4
    FROM (
        select elr.created_on , status, cm.lending_partner_id ,elr.last_updated_on , elr.created_on + INTERVAL '2 hours' as settlement_expected_by 
from enhance_limit_request elr left join credits cm on elr.credit_id = cm.credit_id
    ) AS subquery group by lending_partner_id
)
SELECT 
    COALESCE(s.update_date, ns.update_date) AS update_date,
    COALESCE(s.lending_partner_id, ns.lending_partner_id) as lender_id,
    'ENHANCEMENTS' as CHANNEL_TYPE,
    --COALESCE(s.total_success_today, 0) AS total_success_today,
    --COALESCE(s.sla_breached, 0) AS success_sla_breached,
    COALESCE(s.within_sla, 0) AS success_within_sla,
    COALESCE(s.L1, 0) AS success_L1,
    COALESCE(s.L2, 0) AS success_L2,
    COALESCE(s.L3, 0) AS success_L3,
    --COALESCE(ns.total_non_success, 0) AS total_non_success,
    --COALESCE(ns.sla_breached, 0) AS non_success_sla_breached,
    COALESCE(ns.within_sla, 0) AS non_success_within_sla,
    COALESCE(ns.L1, 0) AS non_success_L1,
    COALESCE(ns.L2, 0) AS non_success_L2,
    COALESCE(ns.L3, 0) AS non_success_L3,
    COALESCE(ns.L4, 0) AS non_success_L4
FROM success_query s
FULL OUTER JOIN non_success_query ns ON s.update_date = ns.update_date and s.lending_partner_id = ns.lending_partner_id
ORDER BY update_date)
union ALL
(
   WITH success_query AS (
    SELECT 
        DATE_TRUNC('day', last_updated_on) AS update_date,lending_partner_id,
        COUNT(CASE 
            WHEN status = 'SETTLED' THEN 1 
            ELSE NULL 
        END) AS total_success_today,
        COUNT(CASE 
            WHEN status = 'SETTLED' AND last_updated_on > settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS sla_breached,
        COUNT(CASE 
            WHEN status = 'SETTLED' AND last_updated_on <= settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS within_sla,
        COUNT(CASE 
            WHEN status = 'SETTLED' AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 3600 <= 2 THEN 1 
            ELSE NULL 
        END) AS L1,
        COUNT(CASE 
            WHEN status = 'SETTLED' AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 3600 > 2 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 3600 <= 36THEN 1 
            ELSE NULL 
        END) AS L2,
        COUNT(CASE 
            WHEN status = 'SETTLED' AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 3600 > 6 THEN 1 
            ELSE NULL 
        END) AS L3
    FROM (
    select c.collection_id, c.created_on,c.collection_status as status, c.last_updated_on , COALESCE(c.settlement_expected_by, c.last_updated_on) as settlement_expected_by, cm.lending_partner_id from collections  c
    left join credits cm on c.credit_id = cm.credit_id
    where c.collection_status not in ('FAILED','REQUESTED','CANCELLED')
    ) AS subquery
    WHERE DATE_TRUNC('day', last_updated_on) = CURRENT_DATE
    GROUP BY DATE_TRUNC('day', last_updated_on),lending_partner_id
),
non_success_query AS (
    SELECT 
        DATE_TRUNC('day', current_timestamp) AS update_date,lending_partner_id,
        COUNT(CASE 
            WHEN status != 'SETTLED' THEN 1 
            ELSE NULL 
        END) AS total_non_success,
        COUNT(CASE 
            WHEN status != 'SETTLED' AND current_timestamp > settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS sla_breached,
        COUNT(CASE 
            WHEN status != 'SETTLED' AND current_timestamp <= settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS within_sla,
        COUNT(CASE 
            WHEN status != 'SETTLED' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 1 THEN 1 
            ELSE NULL 
        END) AS L1,
        COUNT(CASE 
            WHEN status != 'SETTLED' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 > 1 AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 3 THEN 1 
            ELSE NULL 
        END) AS L2,
        COUNT(CASE 
            WHEN status != 'SETTLED' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 > 3 AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 5 THEN 1 
            ELSE NULL 
        END) AS L3,
        COUNT(CASE 
            WHEN status != 'SETTLED' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 > 5 THEN 1 
            ELSE NULL 
        END) AS L4
    FROM (
    select c.collection_id, c.created_on,c.collection_status as status, c.last_updated_on , COALESCE(c.settlement_expected_by, c.last_updated_on) as settlement_expected_by, cm.lending_partner_id from collections  c
    left join credits cm on c.credit_id = cm.credit_id
    where c.collection_status not in ('FAILED','REQUESTED','CANCELLED')
    ) AS subquery group by lending_partner_id
)
SELECT 
    COALESCE(s.update_date, ns.update_date) AS update_date,
    COALESCE(s.lending_partner_id, ns.lending_partner_id) as lender_id,
    'REPAYMENTS' as CHANNEL_TYPE,
    --COALESCE(s.total_success_today, 0) AS total_success_today,
    --COALESCE(s.sla_breached, 0) AS success_sla_breached,
    COALESCE(s.within_sla, 0) AS success_within_sla,
    COALESCE(s.L1, 0) AS success_L1,
    COALESCE(s.L2, 0) AS success_L2,
    COALESCE(s.L3, 0) AS success_L3,
    --COALESCE(ns.total_non_success, 0) AS total_non_success,
    --COALESCE(ns.sla_breached, 0) AS non_success_sla_breached,
    COALESCE(ns.within_sla, 0) AS non_success_within_sla,
    COALESCE(ns.L1, 0) AS non_success_L1,
    COALESCE(ns.L2, 0) AS non_success_L2,
    COALESCE(ns.L3, 0) AS non_success_L3,
    COALESCE(ns.L4, 0) AS non_success_L4
FROM success_query s
FULL OUTER JOIN non_success_query ns ON s.update_date = ns.update_date and s.lending_partner_id = ns.lending_partner_id
ORDER BY update_date)
union ALL
(
   WITH success_query AS (
    SELECT 
        DATE_TRUNC('day', last_updated_on) AS update_date,lending_partner_id,
        COUNT(CASE 
            WHEN status = 'SETTLED' THEN 1 
            ELSE NULL 
        END) AS total_success_today,
        COUNT(CASE 
            WHEN status = 'SETTLED' AND last_updated_on > settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS sla_breached,
        COUNT(CASE 
            WHEN status = 'SETTLED' AND last_updated_on <= settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS within_sla,
        COUNT(CASE 
            WHEN status = 'SETTLED' AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 3600 <= 2 THEN 1 
            ELSE NULL 
        END) AS L1,
        COUNT(CASE 
            WHEN status = 'SETTLED' AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 3600 > 2 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 3600 <= 6 THEN 1 
            ELSE NULL 
        END) AS L2,
        COUNT(CASE 
            WHEN status = 'SETTLED' AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 3600 > 6 THEN 1 
            ELSE NULL 
        END) AS L3
    FROM (
    select disbursal_id, requested_on as created_on, disbursal_status  as status, d.last_updated_on, COALESCE(expected_transfer_by, d.last_updated_on) as settlement_expected_by ,cm.lending_partner_id from disbursals d 
    left join credits cm on d.credit_id = cm.credit_id
    where disbursal_status NOT IN ('FAILED','REJECTED')
    ) AS subquery
    WHERE DATE_TRUNC('day', last_updated_on) = CURRENT_DATE
    GROUP BY DATE_TRUNC('day', last_updated_on),lending_partner_id
),
non_success_query AS (
    SELECT 
        DATE_TRUNC('day', current_timestamp) AS update_date,lending_partner_id,
        COUNT(CASE 
            WHEN status != 'SETTLED' THEN 1 
            ELSE NULL 
        END) AS total_non_success,
        COUNT(CASE 
            WHEN status != 'SETTLED' AND current_timestamp > settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS sla_breached,
        COUNT(CASE 
            WHEN status != 'SETTLED' AND current_timestamp <= settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS within_sla,
        COUNT(CASE 
            WHEN status != 'SETTLED' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 1 THEN 1 
            ELSE NULL 
        END) AS L1,
        COUNT(CASE 
            WHEN status != 'SETTLED' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 > 1 AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 3 THEN 1 
            ELSE NULL 
        END) AS L2,
        COUNT(CASE 
            WHEN status != 'SETTLED' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 > 3 AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 5 THEN 1 
            ELSE NULL 
        END) AS L3,
        COUNT(CASE 
            WHEN status != 'SETTLED' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 > 5 THEN 1 
            ELSE NULL 
        END) AS L4
    FROM (
    select disbursal_id, requested_on as created_on, disbursal_status  as status, d.last_updated_on, COALESCE(expected_transfer_by, d.last_updated_on) as settlement_expected_by ,cm.lending_partner_id from disbursals d 
    left join credits cm on d.credit_id = cm.credit_id
    where disbursal_status NOT IN ('FAILED','REJECTED')
    ) AS subquery group by lending_partner_id
)
SELECT 
    COALESCE(s.update_date, ns.update_date) AS update_date,
    COALESCE(s.lending_partner_id, ns.lending_partner_id) as lender_id,
    'WITHDRAWALS' as CHANNEL_TYPE,
    --COALESCE(s.total_success_today, 0) AS total_success_today,
    --COALESCE(s.sla_breached, 0) AS success_sla_breached,
    COALESCE(s.within_sla, 0) AS success_within_sla,
    COALESCE(s.L1, 0) AS success_L1,
    COALESCE(s.L2, 0) AS success_L2,
    COALESCE(s.L3, 0) AS success_L3,
    --COALESCE(ns.total_non_success, 0) AS total_non_success,
    --COALESCE(ns.sla_breached, 0) AS non_success_sla_breached,
    COALESCE(ns.within_sla, 0) AS non_success_within_sla,
    COALESCE(ns.L1, 0) AS non_success_L1,
    COALESCE(ns.L2, 0) AS non_success_L2,
    COALESCE(ns.L3, 0) AS non_success_L3,
    COALESCE(ns.L4, 0) AS non_success_L4
FROM success_query s
FULL OUTER JOIN non_success_query ns ON s.update_date = ns.update_date and s.lending_partner_id = ns.lending_partner_id
ORDER BY update_date)


                                """, conn) 
        conn.close() 
    
    customer_success_lms.replace(to_replace=pd.NA, value='', inplace=True)
    
    
    
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

        customer_success_audit= pd.read_sql_query("""
             
      
   (WITH success_query AS (
    SELECT 
        DATE_TRUNC('day', last_updated_on) AS update_date,lender_id,
        COUNT(CASE 
            WHEN status = 'SUCCESS' THEN 1 
            ELSE NULL 
        END) AS total_success_today,
        COUNT(CASE 
            WHEN status = 'SUCCESS' AND last_updated_on > settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS sla_breached,
        COUNT(CASE 
            WHEN status = 'SUCCESS' AND last_updated_on <= settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS within_sla,
        COUNT(CASE 
            WHEN status = 'SUCCESS' AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 <= 1 THEN 1 
            ELSE NULL 
        END) AS L1,
        COUNT(CASE 
            WHEN status = 'SUCCESS' AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 > 1 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 <= 3 THEN 1 
            ELSE NULL 
        END) AS L2,
        COUNT(CASE 
            WHEN status = 'SUCCESS' AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 > 3 THEN 1 
            ELSE NULL 
        END) AS L3
    FROM (
        SELECT created_on, last_updated_on, status, lender_id,
               CASE 
                   WHEN lender_id = 'Tata' THEN created_on + INTERVAL '7 days'
                   WHEN lender_id = 'Bajaj' THEN created_on + INTERVAL '5 days'
                   ELSE NULL 
               END AS settlement_expected_by
        FROM foreclosure_requests where lender_id is not null
    ) AS subquery
    WHERE DATE_TRUNC('day', last_updated_on) = CURRENT_DATE
    GROUP BY DATE_TRUNC('day', last_updated_on), lender_id
),
non_success_query AS (
    SELECT 
        DATE_TRUNC('day', current_timestamp) AS update_date,lender_id,
        COUNT(CASE 
            WHEN status != 'SUCCESS' THEN 1 
            ELSE NULL 
        END) AS total_non_success,
        COUNT(CASE 
            WHEN status != 'SUCCESS' AND current_timestamp > settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS sla_breached,
        COUNT(CASE 
            WHEN status != 'SUCCESS' AND current_timestamp <= settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS within_sla,
        COUNT(CASE 
            WHEN status != 'SUCCESS' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 1 THEN 1 
            ELSE NULL 
        END) AS L1,
        COUNT(CASE 
            WHEN status != 'SUCCESS' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 > 1 AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 3 THEN 1 
            ELSE NULL 
        END) AS L2,
        COUNT(CASE 
            WHEN status != 'SUCCESS' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 > 3 AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 5 THEN 1 
            ELSE NULL 
        END) AS L3,
        COUNT(CASE 
            WHEN status != 'SUCCESS' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 > 5 THEN 1 
            ELSE NULL 
        END) AS L4
    FROM (
        SELECT created_on, last_updated_on, status, lender_id,
               CASE 
                   WHEN lender_id = 'Tata' THEN created_on + INTERVAL '7 days'
                   WHEN lender_id = 'Bajaj' THEN created_on + INTERVAL '5 days'
                   ELSE NULL 
               END AS settlement_expected_by
        FROM foreclosure_requests where lender_id is not null
    ) AS subquery group by lender_id
)
SELECT 
    COALESCE(s.update_date, ns.update_date) AS update_date,
    COALESCE(s.lender_id, ns.lender_id) as lender_id,
    'FORECLOSURE' as CHANNEL_TYPE,
    --COALESCE(s.total_success_today, 0) AS total_success_today,
    --COALESCE(s.sla_breached, 0) AS success_sla_breached,
    COALESCE(s.within_sla, 0) AS success_within_sla,
    COALESCE(s.L1, 0) AS success_L1,
    COALESCE(s.L2, 0) AS success_L2,
    COALESCE(s.L3, 0) AS success_L3,
    --COALESCE(ns.total_non_success, 0) AS total_non_success,
    --COALESCE(ns.sla_breached, 0) AS non_success_sla_breached,
    COALESCE(ns.within_sla, 0) AS non_success_within_sla,
    COALESCE(ns.L1, 0) AS non_success_L1,
    COALESCE(ns.L2, 0) AS non_success_L2,
    COALESCE(ns.L3, 0) AS non_success_L3,
    COALESCE(ns.L4, 0) AS non_success_L4
FROM success_query s
FULL OUTER JOIN non_success_query ns ON s.update_date = ns.update_date and s.lender_id = ns.lender_id
ORDER BY update_date)
union ALL   
 (  WITH success_query AS (
    SELECT 
        DATE_TRUNC('day', last_updated_on) AS update_date,lending_partner_id,
        COUNT(CASE 
            WHEN status = 'SUCCESS' THEN 1 
            ELSE NULL 
        END) AS total_success_today,
        COUNT(CASE 
            WHEN status = 'SUCCESS' AND last_updated_on > settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS sla_breached,
        COUNT(CASE 
            WHEN status = 'SUCCESS' AND last_updated_on <= settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS within_sla,
        COUNT(CASE 
            WHEN status = 'SUCCESS' AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 <= 1 THEN 1 
            ELSE NULL 
        END) AS L1,
        COUNT(CASE 
            WHEN status = 'SUCCESS' AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 > 1 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 <= 3 THEN 1 
            ELSE NULL 
        END) AS L2,
        COUNT(CASE 
            WHEN status = 'SUCCESS' AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 > 3 THEN 1 
            ELSE NULL 
        END) AS L3
    FROM (
        select rr.created_on as created_on, rr.last_updated_on, status,cm.lending_partner_id,
  case when cm.lending_partner_id = 'Bajaj' then rr.created_on + INTERVAL '7 days'
   when cm.lending_partner_id = 'Tata' then rr.created_on + INTERVAL '5 days'
  else rr.last_updated_on 
  end as settlement_expected_by
  from revocation_requests rr left join 
  credit_main cm on rr.credit_id = cm.credit_id  
    ) AS subquery
    WHERE DATE_TRUNC('day', last_updated_on) = CURRENT_DATE
    GROUP BY DATE_TRUNC('day', last_updated_on), lending_partner_id
),
non_success_query AS (
    SELECT 
        DATE_TRUNC('day', current_timestamp) AS update_date,lending_partner_id,
        COUNT(CASE 
            WHEN status != 'SUCCESS' THEN 1 
            ELSE NULL 
        END) AS total_non_success,
        COUNT(CASE 
            WHEN status != 'SUCCESS' AND current_timestamp > settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS sla_breached,
        COUNT(CASE 
            WHEN status != 'SUCCESS' AND current_timestamp <= settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS within_sla,
        COUNT(CASE 
            WHEN status != 'SUCCESS' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 1 THEN 1 
            ELSE NULL 
        END) AS L1,
        COUNT(CASE 
            WHEN status != 'SUCCESS' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 > 1 AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 3 THEN 1 
            ELSE NULL 
        END) AS L2,
        COUNT(CASE 
            WHEN status != 'SUCCESS' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 > 3 AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 <= 5 THEN 1 
            ELSE NULL 
        END) AS L3,
        COUNT(CASE 
            WHEN status != 'SUCCESS' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated_on)) / 86400 > 5 THEN 1 
            ELSE NULL 
        END) AS L4
    FROM (
        select rr.created_on as created_on, rr.last_updated_on, status,lending_partner_id,
  case when cm.lending_partner_id = 'Bajaj' then rr.created_on + INTERVAL '7 days'
   when cm.lending_partner_id = 'Tata' then rr.created_on + INTERVAL '5 days'
  else rr.last_updated_on 
  end as settlement_expected_by
  from revocation_requests rr left join 
  credit_main cm on rr.credit_id = cm.credit_id   where cm.lending_partner_id is not null
    ) AS subquery group by lending_partner_id
)
SELECT 
    COALESCE(s.update_date, ns.update_date) AS update_date,
    COALESCE(s.lending_partner_id, ns.lending_partner_id) as lender_id,
    'LIEN REMOVAL' as CHANNEL_TYPE,
    --COALESCE(s.total_success_today, 0) AS total_success_today,
    --COALESCE(s.sla_breached, 0) AS success_sla_breached,
    COALESCE(s.within_sla, 0) AS success_within_sla,
    COALESCE(s.L1, 0) AS success_L1,
    COALESCE(s.L2, 0) AS success_L2,
    COALESCE(s.L3, 0) AS success_L3,
    --COALESCE(ns.total_non_success, 0) AS total_non_success,
    --COALESCE(ns.sla_breached, 0) AS non_success_sla_breached,
    COALESCE(ns.within_sla, 0) AS non_success_within_sla,
    COALESCE(ns.L1, 0) AS non_success_L1,
    COALESCE(ns.L2, 0) AS non_success_L2,
    COALESCE(ns.L3, 0) AS non_success_L3,
    COALESCE(ns.L4, 0) AS non_success_L4

FROM success_query s
FULL OUTER JOIN non_success_query ns ON s.update_date = ns.update_date and s.lending_partner_id = ns.lending_partner_id
ORDER BY update_date)
 UNION ALL
 
  ( WITH success_query AS (
    SELECT 
        DATE_TRUNC('day', last_updated_on) AS update_date,lending_partner_id,
        COUNT(CASE 
            WHEN status in ('PENDING_LODGEMENT', 'APPROVED_NOT_DISBURSED','ACTIVE','PENDING_CLOSURE') THEN 1 
            ELSE NULL 
        END) AS total_success_today,
        COUNT(CASE 
            WHEN status in ('PENDING_LODGEMENT', 'APPROVED_NOT_DISBURSED','ACTIVE','PENDING_CLOSURE') AND last_updated_on > settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS sla_breached,
        COUNT(CASE 
            WHEN status in ('PENDING_LODGEMENT', 'APPROVED_NOT_DISBURSED','ACTIVE','PENDING_CLOSURE') AND last_updated_on <= settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS within_sla,
        COUNT(CASE 
            WHEN status in ('PENDING_LODGEMENT', 'APPROVED_NOT_DISBURSED','ACTIVE','PENDING_CLOSURE') AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 <= 1 THEN 1 
            ELSE NULL 
        END) AS L1,
        COUNT(CASE 
            WHEN status in ('PENDING_LODGEMENT', 'APPROVED_NOT_DISBURSED','ACTIVE','PENDING_CLOSURE') AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 > 1 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 <= 3 THEN 1 
            ELSE NULL 
        END) AS L2,
        COUNT(CASE 
            WHEN status in ('PENDING_LODGEMENT', 'APPROVED_NOT_DISBURSED','ACTIVE','PENDING_CLOSURE') AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 > 3 THEN 1 
            ELSE NULL 
        END) AS L3
    FROM (
        
select 
        cm.credit_id, cm.credit_status as status, 
        cm.lending_partner_id, 
    cm.created_on as created_on, account_opening_time_actual as last_updated_on, 
    --(account_opening_time - created_on) as account_opening_turnaround_time, 
    
    case when cm.lending_partner_id = 'Tata' then created_on + INTERVAL '30 minutes' 
        when cm.lending_partner_id = 'Bajaj' then created_on + INTERVAL '4 hours' 
        ELSE NULL 
        END AS settlement_expected_by  ,
        lodgement_time_actual,
        --(lodgement_time- account_opening_time) as lodgement_turnaround_time, 
    case when cm.lending_partner_id = 'Tata' then lodgement_time_actual + INTERVAL '15 minutes' 
        when cm.lending_partner_id = 'Bajaj' then lodgement_time_actual + INTERVAL '2 hours' 
        ELSE NULL 
        END AS settlement_lodgement_time,   
 
    cm.account_id 
from credit_main cm
left join 
(select credit_id, min(last_updated_on) as account_opening_time_actual
    from credit_audit
    where credit_status in ('PENDING_LODGEMENT', 'APPROVED_NOT_DISBURSED','ACTIVE','PENDING_CLOSURE')
    group by credit_id) pl 
    on cm.credit_id = pl.credit_id
left join 
(select credit_id, min(last_updated_on) as lodgement_time_actual
    from credit_audit
    where credit_status IN ('APPROVED_NOT_DISBURSED','ACTIVE','PENDING_CLOSURE')
    group by credit_id) an 
    on cm.credit_id = an.credit_id
left join
(select account_holder_name, account_holderpan, account_id from borrower_accounts) ba
on ba.account_id= cm.account_id
    ) AS subquery
    WHERE DATE_TRUNC('day', last_updated_on) = CURRENT_DATE
    GROUP BY DATE_TRUNC('day', last_updated_on),lending_partner_id
),
non_success_query AS (
    SELECT 
        DATE_TRUNC('day', current_timestamp) AS update_date,lending_partner_id,
        COUNT(CASE 
            WHEN status = 'PENDING_DISBURSAL_APPROVAL' THEN 1 
            ELSE NULL 
        END) AS total_non_success,
        COUNT(CASE 
            WHEN status = 'PENDING_DISBURSAL_APPROVAL' AND current_timestamp > settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS sla_breached,
        COUNT(CASE 
            WHEN status = 'PENDING_DISBURSAL_APPROVAL' AND current_timestamp <= settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS within_sla,
        COUNT(CASE 
            WHEN status = 'PENDING_DISBURSAL_APPROVAL' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_expected_by)) / 86400 <= 1 THEN 1 
            ELSE NULL 
        END) AS L1,
        COUNT(CASE 
            WHEN status = 'PENDING_DISBURSAL_APPROVAL' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_expected_by)) / 86400 > 1 AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_expected_by)) / 86400 <= 3 THEN 1 
            ELSE NULL 
        END) AS L2,
        COUNT(CASE 
            WHEN status = 'PENDING_DISBURSAL_APPROVAL' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_expected_by)) / 86400 > 3 AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_expected_by)) / 86400 <= 5 THEN 1 
            ELSE NULL 
        END) AS L3,
        COUNT(CASE 
            WHEN status = 'PENDING_DISBURSAL_APPROVAL' AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_expected_by)) / 86400 > 5 THEN 1 
            ELSE NULL 
        END) AS L4
    FROM (
        
select 
        cm.credit_id, cm.credit_status as status, 
        cm.lending_partner_id, 
    cm.created_on as created_on, account_opening_time_actual as last_updated_on, 
    --(account_opening_time - created_on) as account_opening_turnaround_time, 
    
    case when cm.lending_partner_id = 'Tata' then created_on + INTERVAL '30 minutes' 
        when cm.lending_partner_id = 'Bajaj' then created_on + INTERVAL '4 hours' 
        ELSE NULL 
        END AS settlement_expected_by  ,
        lodgement_time_actual,
        --(lodgement_time- account_opening_time) as lodgement_turnaround_time, 
    case when cm.lending_partner_id = 'Tata' then lodgement_time_actual + INTERVAL '15 minutes' 
        when cm.lending_partner_id = 'Bajaj' then lodgement_time_actual + INTERVAL '2 hours' 
        ELSE NULL 
        END AS settlement_lodgement_time,   
 
    cm.account_id 
from credit_main cm
left join 
(select credit_id, min(last_updated_on) as account_opening_time_actual
    from credit_audit
    where credit_status in ('PENDING_LODGEMENT', 'APPROVED_NOT_DISBURSED','ACTIVE','PENDING_CLOSURE')
    group by credit_id) pl 
    on cm.credit_id = pl.credit_id
left join 
(select credit_id, min(last_updated_on) as lodgement_time_actual
    from credit_audit
    where credit_status IN ('APPROVED_NOT_DISBURSED','ACTIVE','PENDING_CLOSURE')
    group by credit_id) an 
    on cm.credit_id = an.credit_id
left join
(select account_holder_name, account_holderpan, account_id from borrower_accounts) ba
on ba.account_id= cm.account_id
    ) AS subquery group by lending_partner_id
    
)
SELECT 
    COALESCE(s.update_date, ns.update_date) AS update_date,
    COALESCE(s.lending_partner_id, ns.lending_partner_id) as lender_id,
    'ACCOUNT OPENING' as CHANNEL_TYPE,
    --COALESCE(s.total_success_today, 0) AS total_success_today,
    --COALESCE(s.sla_breached, 0) AS success_sla_breached,
    COALESCE(s.within_sla, 0) AS success_within_sla,
    COALESCE(s.L1, 0) AS success_L1,
    COALESCE(s.L2, 0) AS success_L2,
    COALESCE(s.L3, 0) AS success_L3,
    --COALESCE(ns.total_non_success, 0) AS total_non_success,
    --COALESCE(ns.sla_breached, 0) AS non_success_sla_breached,
    COALESCE(ns.within_sla, 0) AS non_success_within_sla,
    COALESCE(ns.L1, 0) AS non_success_L1,
    COALESCE(ns.L2, 0) AS non_success_L2,
    COALESCE(ns.L3, 0) AS non_success_L3,
    COALESCE(ns.L4, 0) AS non_success_L4
FROM success_query s
FULL OUTER JOIN non_success_query ns ON s.update_date = ns.update_date and s.lending_partner_id = ns.lending_partner_id
ORDER BY update_date)
  UNION ALL
( WITH success_query AS (
    SELECT 
        DATE_TRUNC('day', last_updated_on) AS update_date,lending_partner_id,
        COUNT(CASE 
            WHEN status in ('ACTIVE','PENDING_CLOSURE','CLOSED') THEN 1 
            ELSE NULL 
        END) AS total_success_today,
        COUNT(CASE 
            WHEN status in ('ACTIVE','PENDING_CLOSURE','CLOSED') AND last_updated_on > settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS sla_breached,
        COUNT(CASE 
            WHEN status in ('ACTIVE','PENDING_CLOSURE','CLOSED') AND last_updated_on <= settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS within_sla,
        COUNT(CASE 
            WHEN status in ('ACTIVE','PENDING_CLOSURE','CLOSED') AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 <= 1 THEN 1 
            ELSE NULL 
        END) AS L1,
        COUNT(CASE 
            WHEN status in ('ACTIVE','PENDING_CLOSURE','CLOSED') AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 > 1 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 <= 3 THEN 1 
            ELSE NULL 
        END) AS L2,
        COUNT(CASE 
            WHEN status in ('ACTIVE','PENDING_CLOSURE','CLOSED') AND EXTRACT(EPOCH FROM (last_updated_on - created_on)) / 3600 > EXTRACT(EPOCH FROM (settlement_expected_by - created_on)) / 3600 AND EXTRACT(EPOCH FROM (last_updated_on - settlement_expected_by)) / 86400 > 3 THEN 1 
            ELSE NULL 
        END) AS L3
    FROM (
        
select 
        cm.credit_id, cm.credit_status as STATUS, 
        cm.lending_partner_id, 
    cm.created_on as created_on, account_opening_time_actual, 
    --(account_opening_time - created_on) as account_opening_turnaround_time, 
    
    case when cm.lending_partner_id = 'Tata' then created_on + INTERVAL '15 minutes' 
        when cm.lending_partner_id = 'Bajaj' then created_on + INTERVAL '2 hours' 
        ELSE NULL 
        END AS settlement_account_opening_time,
        lodgement_time_actual as last_updated_on,
        --(lodgement_time- account_opening_time) as lodgement_turnaround_time, 
    case when cm.lending_partner_id = 'Tata'  and lodgement_time_actual is not null then lodgement_time_actual + INTERVAL '15 minutes' 
        when cm.lending_partner_id = 'Bajaj' and lodgement_time_actual is not null then lodgement_time_actual + INTERVAL '2 hours' 
         when cm.lending_partner_id = 'Tata' and lodgement_time_actual is null and account_opening_time_actual is not null then account_opening_time_actual + INTERVAL '30 minutes' 
        when cm.lending_partner_id = 'Bajaj' and lodgement_time_actual is null and account_opening_time_actual is not null then account_opening_time_actual + INTERVAL '4 hours' 
         when cm.lending_partner_id = 'Tata' and lodgement_time_actual is null and account_opening_time_actual is  null then cm.created_on + INTERVAL '30 minutes' 
        when cm.lending_partner_id = 'Bajaj' and lodgement_time_actual is null and account_opening_time_actual is  null then cm.created_on + INTERVAL '4 hours' 
        ELSE NULL 
        END AS settlement_expected_by,  
 
    cm.account_id 
from credit_main cm
left join 
(select credit_id, min(last_updated_on) as account_opening_time_actual
    from credit_audit
    where credit_status in ('PENDING_LODGEMENT', 'APPROVED_NOT_DISBURSED','ACTIVE','PENDING_CLOSURE')
    group by credit_id) pl 
    on cm.credit_id = pl.credit_id
left join 
(select credit_id, min(last_updated_on) as lodgement_time_actual
    from credit_audit
    where credit_status IN ('APPROVED_NOT_DISBURSED','ACTIVE','PENDING_CLOSURE')
    group by credit_id) an 
    on cm.credit_id = an.credit_id
left join
(select account_holder_name, account_holderpan, account_id from borrower_accounts) ba
on ba.account_id= cm.account_id
    ) AS subquery
    WHERE DATE_TRUNC('day', last_updated_on) = CURRENT_DATE
    GROUP BY DATE_TRUNC('day', last_updated_on),lending_partner_id
),
non_success_query AS (
    SELECT 
        DATE_TRUNC('day', current_timestamp) AS update_date,lending_partner_id,
        COUNT(CASE 
            WHEN status = 'PENDING_LODGEMENT'  THEN 1 
            ELSE NULL 
        END) AS total_non_success,
        COUNT(CASE 
            WHEN status  = 'PENDING_LODGEMENT'  AND current_timestamp > settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS sla_breached,
        COUNT(CASE 
            WHEN status  = 'PENDING_LODGEMENT'  AND current_timestamp <= settlement_expected_by THEN 1 
            ELSE NULL 
        END) AS within_sla,
        COUNT(CASE 
            WHEN status  = 'PENDING_LODGEMENT'  AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_expected_by)) / 86400 <= 1 THEN 1 
            ELSE NULL 
        END) AS L1,
        COUNT(CASE 
            WHEN status  = 'PENDING_LODGEMENT'  AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_expected_by)) / 86400 > 1 AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_expected_by)) / 86400 <= 3 THEN 1 
            ELSE NULL 
        END) AS L2,
        COUNT(CASE 
            WHEN status  = 'PENDING_LODGEMENT'  AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_expected_by)) / 86400 > 3 AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_expected_by)) / 86400 <= 5 THEN 1 
            ELSE NULL 
        END) AS L3,
        COUNT(CASE 
            WHEN status  = 'PENDING_LODGEMENT'  AND current_timestamp > settlement_expected_by AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - settlement_expected_by)) / 86400 > 5 THEN 1 
            ELSE NULL 
        END) AS L4
    FROM (
        
select 
        cm.credit_id, cm.credit_status as STATUS,         cm.lending_partner_id, 
    cm.created_on as created_on, account_opening_time_actual, 
    --(account_opening_time - created_on) as account_opening_turnaround_time, 
    
    case when cm.lending_partner_id = 'Tata' then created_on + INTERVAL '15 minutes' 
        when cm.lending_partner_id = 'Bajaj' then created_on + INTERVAL '2 hours' 
        ELSE NULL 
        END AS settlement_account_opening_time,
        lodgement_time_actual as last_updated_on,
        --(lodgement_time- account_opening_time) as lodgement_turnaround_time, 
    case when cm.lending_partner_id = 'Tata'  and lodgement_time_actual is not null then lodgement_time_actual + INTERVAL '15 minutes' 
        when cm.lending_partner_id = 'Bajaj' and lodgement_time_actual is not null then lodgement_time_actual + INTERVAL '2 hours' 
         when cm.lending_partner_id = 'Tata' and lodgement_time_actual is null and account_opening_time_actual is not null then account_opening_time_actual + INTERVAL '30 minutes' 
        when cm.lending_partner_id = 'Bajaj' and lodgement_time_actual is null and account_opening_time_actual is not null then account_opening_time_actual + INTERVAL '4 hours' 
         when cm.lending_partner_id = 'Tata' and lodgement_time_actual is null and account_opening_time_actual is  null then cm.created_on + INTERVAL '30 minutes' 
        when cm.lending_partner_id = 'Bajaj' and lodgement_time_actual is null and account_opening_time_actual is  null then cm.created_on + INTERVAL '4 hours' 
        ELSE NULL 
        END AS settlement_expected_by,  
 
    cm.account_id 
from credit_main cm
left join 
(select credit_id, min(last_updated_on) as account_opening_time_actual
    from credit_audit
    where credit_status in ('PENDING_LODGEMENT', 'APPROVED_NOT_DISBURSED','ACTIVE','PENDING_CLOSURE')
    group by credit_id) pl 
    on cm.credit_id = pl.credit_id
left join 
(select credit_id, min(last_updated_on) as lodgement_time_actual
    from credit_audit
    where credit_status IN ('APPROVED_NOT_DISBURSED','ACTIVE','PENDING_CLOSURE')
    group by credit_id) an 
    on cm.credit_id = an.credit_id
left join
(select account_holder_name, account_holderpan, account_id from borrower_accounts) ba
on ba.account_id= cm.account_id
    ) AS subquery group by lending_partner_id
    
)
SELECT 
    COALESCE(s.update_date, ns.update_date) AS update_date,
    COALESCE(s.lending_partner_id, ns.lending_partner_id) as lender_id,
    'LODGEMENT' as CHANNEL_TYPE,
    --COALESCE(s.total_success_today, 0) AS total_success_today,
    --COALESCE(s.sla_breached, 0) AS success_sla_breached,
    COALESCE(s.within_sla, 0) AS success_within_sla,
    COALESCE(s.L1, 0) AS success_L1,
    COALESCE(s.L2, 0) AS success_L2,
    COALESCE(s.L3, 0) AS success_L3,
    --COALESCE(ns.total_non_success, 0) AS total_non_success,
    --COALESCE(ns.sla_breached, 0) AS non_success_sla_breached,
    COALESCE(ns.within_sla, 0) AS non_success_within_sla,
    COALESCE(ns.L1, 0) AS non_success_L1,
    COALESCE(ns.L2, 0) AS non_success_L2,
    COALESCE(ns.L3, 0) AS non_success_L3,
    COALESCE(ns.L4, 0) AS non_success_L4
FROM success_query s
FULL OUTER JOIN non_success_query ns ON s.update_date = ns.update_date and s.lending_partner_id = ns.lending_partner_id
ORDER BY update_date)

                                """, conn) 
        conn.close() 
    
    customer_success_audit.replace(to_replace=pd.NA, value='', inplace=True)
    
    final_df_aggregate = pd.concat([customer_success_lms, customer_success_audit])
    
    final_df_aggregate = final_df_aggregate[['channel_type','success_within_sla','success_l1','success_l2','success_l3','non_success_within_sla','non_success_l1','non_success_l2','non_success_l3','non_success_l4','lender_id']]

    
    rename_mapping = {
                    
                    'channel_type': 'Request Type',
                    'success_within_sla': 'Resolved within SLA',
                    'success_l1': 'L1 Resolved SLA Breached',
                    'success_l2': 'L2 Resolved SLA Breached',
                    'success_l3': 'L3 Resolved SLA Breached',
                    'non_success_within_sla': 'Unresolved within SLA',
                    'non_success_l1': '1D Unresolved SLA Breached',
                    'non_success_l2': '3D Unresolved SLA Breached',
                    'non_success_l3': '5D Unresolved SLA Breached',
                    'non_success_l4': '5D+ Unresolved SLA Breached',
                    'lender_id': 'Lender'
                    }
    
    
    
    final_df_aggregate.rename(columns=rename_mapping, inplace=True)
    
    #final_df_aggregate['Date'] = final_df_aggregate['Date'].dt.date

    
    return withdrawal_data, collections_data, lien_removal_data, foreclosure_data, account_opening_data, lodgement_data, enhancement_request_data, final_df_aggregate
    #return  final_df_aggregate


if __name__ == "__main__":
    main()
