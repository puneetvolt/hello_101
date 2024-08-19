import pandas as pd
import psycopg2
import warnings
import sys
sys.path.append("/Users/puneet/Code/")
from sshtunnel import SSHTunnelForwarder
from configurations_dir import local_db_creds as creds
from configurations_dir.hosts import AWSCredentials

# Suppress warnings
warnings.filterwarnings("ignore")

def connect_to_db(database, user, password, host, port):
    try:
        connection = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port
        )
        connection.autocommit = True
        cursor = connection.cursor()
        return connection, cursor
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def close_db_connection(connection):
    if connection:
        connection.close()

def calculation_transactions(connection):
    if connection is None:
        print("No database connection.")
        return None
    try:
        query = """
            with payout_transactionsCTE as (
                select destination_id as partner_account_id, payment_date, credit_account_number
                from payout_transactions pt
                where payment_date is not null
            )
            select partner_account_id, credit_account_number, min(payment_date) as first_payment_date, 
                    max(payment_date) as last_payment_date
            from payout_transactionsCTE
                group by partner_account_id, credit_account_number
        """
        data = pd.read_sql_query(query, connection)
        return data
    except Exception as e:
        print(f"Error reading data: {e}")
        return None

def partner_accounts(conn):
    if conn is None:
        print("No database connection.")
        return None
    try:
        partner_accounts = """
                WITH PartnerAccountsCTE AS (
                    SELECT 
                        account_id, 
                        partner_name, 
                        bank_account_number, 
                        bank_accountifsccode 
                    FROM
                        partner_accounts pa
                ),
                ActivatedPartner AS (
                    SELECT 
                        partner_account_id, 
                        CAST(MIN(created_on) AT TIME ZONE 'Asia/Kolkata' AS DATE) AS activation_date 
                    FROM credit_main
                    GROUP BY partner_account_id
                )
                SELECT 
                    pa.account_id as partner_account_id, 
                    pa.partner_name, 
                    pa.bank_account_number, 
                    pa.bank_accountifsccode, 
                    ap.activation_date,
                    CASE 
                        WHEN ap.activation_date IS NULL THEN 'empanelled'
                        ELSE 'activated'
                    END AS account_status
                FROM PartnerAccountsCTE pa
                LEFT JOIN ActivatedPartner ap 
                ON pa.account_id = ap.partner_account_id;

        """
        partner_bank_account = pd.read_sql_query(partner_accounts, conn)
        return partner_bank_account
    except Exception as e:
        print(f"Error reading data: {e}")
        return None

def main():
    # Connect to the primary database
    db_params = creds.db_params
    connection, cursor = connect_to_db(**db_params)
    
    if connection:
        data = calculation_transactions(connection)
        close_db_connection(connection)
    
    # Connect to the AWS database
    try:
        with SSHTunnelForwarder(
            (AWSCredentials.host, 22),
            ssh_username=AWSCredentials.ssh_username, 
            ssh_pkey="/Users/puneet/Code/configurations_dir/volt-reporting-tunnel-2.cer",
            remote_bind_address=(AWSCredentials.Voltaudits.blind_address, 5432)
        ) as server:
            server.start()
            print(f"SSH Tunnel established. Forwarded port: {server.local_bind_port}")

            connection_params = AWSCredentials.Voltaudits.params.copy()
            connection_params['port'] = server.local_bind_port

            conn = psycopg2.connect(**connection_params)
            conn.set_session(readonly=True)
            print("AWS DB connected successfully")

            partner_data = partner_accounts(conn)
            partner_data.to_csv("partner_data.csv", index= False)
            print(partner_data)

            close_db_connection(conn)
    except Exception as e:
        print(f"Error connecting to AWS database: {e}")

    dataframe= pd.merge(data, partner_data, left_on= ['partner_account_id'], right_on= ['partner_account_id'], how= 'outer')
    # add brackets and strip bank accounts():
    dataframe['bank_account_number']= dataframe['bank_account_number'].str.strip()
    dataframe['bank_account_number'] = dataframe['bank_account_number'].apply(lambda x: f"[{x}]" if isinstance(x, str) and x.strip() else x)

    dataframe

if __name__ == '__main__':
    main()


