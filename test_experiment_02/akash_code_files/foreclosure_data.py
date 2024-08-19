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

        foreclosure_email= pd.read_sql_query(""" 
                        select fr.collection_id,ba.account_holderpan,ba.account_holder_name,fr.request_id, cm.lender_loan_account_number, 
   cm.lender_credit_id,fr.status,fr.created_on, fr.last_updated_on , fr.lender_id, fr.credit_id, fr.amount_due
      from
							(SELECT account_id, associated_collection_id as collection_id,
									       created_on + INTERVAL '5 hours 30 minutes' AS created_on, 
									       last_updated_on + INTERVAL '5 hours 30 minutes' AS last_updated_on, 
									       lender_id, 
									       request_id, 
									       status, 
									       credit_id, amount_due , lender_loan_id 
									FROM foreclosure_requests
									WHERE created_on + INTERVAL '5 hours 30 minutes' < (CURRENT_DATE + INTERVAL '12 hours 30 minutes')
									  AND created_on + INTERVAL '5 hours 30 minutes' > (CURRENT_DATE - 1 + INTERVAL '12 hours 30 minutes')
									               ) fr
                            left join
                            (select lending_partner_id, account_id, lender_loan_account_number, lender_credit_id, credit_id from credit_main) cm
                            on cm.credit_id = fr.credit_id
                            left join
                            (select account_id,  account_holder_name, account_holderpan, account_holder_phone_number from borrower_accounts) ba
                            on ba.account_id = fr.account_id   
                        """, conn) 
                        
        conn.close() 
        
    print(foreclosure_email)
    collections_ids_tuple = tuple(foreclosure_email['collection_id'])
    placeholders = ', '.join(['%s'] * len(collections_ids_tuple))
        
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

        query = f"""
                SELECT 
            collection_id, 
            actual_amount_collected, 
            transaction_id, 
            COALESCE(
                callback_received_on AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata', 
                last_updated_on AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'
            ) AS callback_received_on
        FROM 
            collections  where collection_id in ({placeholders})  ;

        """ 
                             
        
        print ("Fetching Collections...")
                
        collections_details = pd.read_sql_query(query, conn, params=collections_ids_tuple)
        conn.close()
            
    foreclosure_collections = pd.merge(foreclosure_email,collections_details, on = 'collection_id', how = 'left')
    print()
    foreclosure_collections = foreclosure_collections.rename(columns={
                                    'account_holderpan': 'Customer Pan',
                                    'account_holder_name': 'Customer Name',
                                    'request_id': 'Request ID',
                                    'lender_id': 'Lender',
                                    'lender_loan_account_number': 'Lender Loan Account Number',
                                    'lender_credit_id': 'Lender Credit ID',
                                    'status': 'Status',
                                    'created_on': 'Created_on',
                                    'last_updated_on': 'Last Updated On',
                                    'credit_id': 'Credit ID',
                                    'amount_due': 'Amount Due',
                                    'collection_id': 'Collection ID',
                                    'actual_amount_collected':'Amount Collected',
                                    'transaction_id':'Transaction ID',
                                    'callback_received_on':'Callback Received On'
                                                        })
    
    foreclosure_collections_tata = foreclosure_collections[foreclosure_collections['Lender'] == 'Tata']
    foreclosure_collections_Bajaj = foreclosure_collections[foreclosure_collections['Lender'] == 'Bajaj']
    
    print(foreclosure_collections_tata)
    print(foreclosure_collections_Bajaj)


    return foreclosure_collections_tata, foreclosure_collections_Bajaj

if __name__ == '__main__':
    main()
