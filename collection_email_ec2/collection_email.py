import psycopg2
from sshtunnel import SSHTunnelForwarder
import pygsheets
import pandas as pd
from datetime import date, timedelta, datetime
import gspread_dataframe as gd
import warnings
warnings.filterwarnings("ignore")
import sys
sys.path.append("/home/ec2-user/")
from configurations_dir.hosts import AWSCredentials
from datetime import date, timedelta, datetime

def main():
    yesterday = datetime.today() - timedelta(days=1)
    yesterday_date = yesterday.date()
    yesterday_datetime = datetime(yesterday.year, yesterday.month, yesterday.day, 9, 0)
    yesterday_date_str = yesterday_date.strftime('%Y-%m-%d')
    yesterday_datetime_str = yesterday_datetime.strftime('%Y-%m-%d %H:%M:%S')

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

        print("DB Connected")

        credits_data = pd.read_sql_query(f"""
                        SELECT col.collection_id, crd.lender_credit_id as LAN, 
                            col.payment_date, col.actual_amount_collected, 
                            col.collection_type, col.bank_ref_no, 
                            col.payment_mode, col.pg_tracking_id, 
                            col.status_message, crd.account_id
                        FROM
                        (SELECT collection_id, credit_id, actual_amount_collected, collection_status, payment_date,
                                CAST(pg_response AS JSONB)->>'bankRefNo' AS bank_ref_no,  
                                CAST(pg_response AS JSONB)->>'paymentMode' AS payment_mode,  
                                CAST(pg_response AS JSONB)->>'pgTrackingId' AS pg_tracking_id,  
                                CAST(pg_response AS JSONB)->>'statusMessage' AS status_message, collection_type
                        FROM collections 
                        WHERE collection_status in ('COLLECTION_COMPLETE','SETTLED')
                        AND (CAST(payment_date AS DATE) = '{yesterday_date_str}'
                            OR (CAST(payment_date AS DATE) < '{yesterday_date_str}'
                            AND last_updated_on > '{yesterday_datetime_str}'))) col
                        LEFT JOIN
                        (SELECT credit_id, account_id, lender_credit_id, lending_partner_id FROM credits) crd
                        ON col.credit_id = crd.credit_id 
                        WHERE crd.lending_partner_id = 'Bajaj'
                    """, conn)

    # Voltaudits
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

        print("DB Connected")

        borrower_accounts= pd.read_sql_query(""" 
                        select account_id, account_holder_name from borrower_accounts
                        """, conn) 

        merged_df= pd.merge(left = credits_data, right= borrower_accounts, on = 'account_id', how= 'left')
        merged_df= merged_df.reset_index(drop= True)

        merged_df= merged_df[['collection_id', 'lan', 'account_holder_name', 'payment_date', 'actual_amount_collected', 'collection_type', 'bank_ref_no', 'payment_mode', 'pg_tracking_id']]
        return merged_df

if __name__ == '__main__':
    main()
