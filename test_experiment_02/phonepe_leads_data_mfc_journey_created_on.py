""".PhonePe leads data():

Changes:
    1. Update OTP Email Velidation():

"""

import psycopg2
from sshtunnel import SSHTunnelForwarder
import pygsheets
import pandas as pd
from datetime import date, timedelta, datetime
import gspread_dataframe as gd
import sys
# sys.path.append("/home/ec2-user/")
sys.path.append("/Users/puneet/Code/")
from configurations_dir.hosts import AWSCredentials
import numpy as np
from datetime import datetime

def get_db_data():

    # VOLT_LMS
    with SSHTunnelForwarder(
        (AWSCredentials.host, 22),
        ssh_username= AWSCredentials.ssh_username, 
        # ssh_pkey="/home/ec2-user/configurations_dir/volt-reporting-tunnel-2.cer",
        ssh_pkey="/Users/puneet/Code/configurations_dir/volt-reporting-tunnel-2.cer", 
        remote_bind_address=(AWSCredentials.Voltlms.blind_address, 5432)
    ) as server:
        server.start()

        connection_params = AWSCredentials.Voltlms.params.copy()
        connection_params['port'] = server.local_bind_port

        # Now use connection_params to make the connection
        conn = psycopg2.connect(**connection_params)
        conn.set_session(readonly=True)
        print ("DB Connected")

        leads_portfolio_data= pd.read_sql_query("""
            select 
                lead_id, 
                total_portfolio_value as mfc_total_portfolio_value, 
                eligible_portfolio_value as mfc_eligible_portfolio_value, 
                eligible_credit_limit as mfc_eligible_credit_limit, 
                last_updated_on, created_on
                from leads_portfolio_data
                                """, conn) 
        conn.close() 

    # VOLT_AUDITS
    with SSHTunnelForwarder(
        (AWSCredentials.host, 22),
        ssh_username= AWSCredentials.ssh_username, 
        # ssh_pkey="/home/ec2-user/configurations_dir/volt-reporting-tunnel-2.cer",
        ssh_pkey="/Users/puneet/Code/configurations_dir/volt-reporting-tunnel-2.cer",
        remote_bind_address=(AWSCredentials.Voltaudits.blind_address, 5432)
    ) as server:
        server.start()

        connection_params = AWSCredentials.Voltaudits.params.copy()
        connection_params['port'] = server.local_bind_port

        conn = psycopg2.connect(**connection_params)
        conn.set_session(readonly=True)
        print ("DB Connected")

        customer_leads= pd.read_sql_query("""
              select lead_id, cl.pan, cl.partner_account_id, cl.platform_account_id, cl.phone_number as mfc_phone_number, 
                pla2.platform_name, pa.partner_name, pa.partner_account_type, mfc_created_on, full_name, email_id, request_id
                from
            (select lead_id, pan, partner_account_id, platform_account_id, phone_number, 
                created_date_time as mfc_created_on, full_name, email_id, request_id
                from customer_leads
                    where platform_account_id = '602f3c59-b135-49cb-bb14-d0c2ba9a13f8') cl
            left join
               (select account_id, platform_name from platform_accounts) pla2
            on pla2.account_id = cl.platform_account_id
            left join
                (select partner_name, account_id, partner_account_type from partner_accounts) pa
            on pa.account_id= cl.partner_account_id
                    """, conn) 
        print("customer_leads checked() ... ")

        application_data= pd.read_sql_query("""
                   select * from
                   (SELECT
                        x.application_id,
                        z.account_holderpan as app_pan,
                        z.account_holder_phone_number as app_phone_number, y.available_portfolio_value as app_available_portfolio_value, 
                        y.eligible_portfolio_value as app_eligible_portfolio_value, y.eligible_credit_limit as app_eligible_credit_limit, 
                        y.pledged_portfolio_value as app_pledged_portfolio_value,
                        y.pledged_credit_limit as app_pledged_credit_limit, coalesce(x.current_step_id, 'COMPLETED') as current_step_id,  x.last_updated_on as app_last_updated_on, 
                        x.platform_account_id, x.partner_account_id, pa.partner_account_type, coalesce(pla.platform_name, 'VOLT') platform_name,
                        pa.partner_name, application_state, completed_on, app_created_on, lender_account_id, account_holder_name, application_type, 
                        afd.cams_fetched, afd.karvy_fetched, account_holder_email, mf_count
                        from
                    (select min(created_on) as app_created_on, account_id
                            from credit_applications_entity
                                    where application_type= 'CREDIT_AGAINST_SECURITIES_BORROWER'
                                    -- and platform_account_id = '602f3c59-b135-49cb-bb14-d0c2ba9a13f8'
                                        group by account_id) cae1
                    inner join
                    (select 
                        application_id, account_id, application_state, created_on, completed_on, 
                            lender_account_id, current_step_id, last_updated_on, application_type, platform_account_id, partner_account_id 
                                from credit_applications_entity
                                    where application_type= 'CREDIT_AGAINST_SECURITIES_BORROWER'
                                    -- and platform_account_id = '602f3c59-b135-49cb-bb14-d0c2ba9a13f8'
                                    ) x
                        on x.account_id= cae1.account_id
                    left join
                    (select application_id, available_portfolio_value, eligible_credit_limit, eligible_portfolio_value, pledged_credit_limit, pledged_portfolio_value 
                        from credit_application_meta_data) y
                    on x.application_id = y.application_id
                    left join
                    (select account_id, account_holder_name, account_holderpan, account_holder_phone_number, account_holder_email from borrower_accounts) z
                    on x.account_id = z.account_id
                    left join
                    (select account_id, partner_name, partner_account_type from partner_accounts) pa
                    on pa.account_id = x.partner_account_id
                    left join
                    (select platform_name, account_id from platform_accounts) as pla
                    on pla.account_id= x.platform_account_id
                    left join
                    (SELECT application_id,
                            MAX(CASE WHEN asset_repository = 'CAMS' THEN 'Y' ELSE 'N' END) AS cams_fetched,
                            MAX(CASE WHEN asset_repository = 'KARVY' THEN 'Y' ELSE 'N' END) AS karvy_fetched
                            FROM asset_fetch_data 
                            group by application_id) afd
                            on afd.application_id = x.application_id
                    left join
                    (select application_id, count(isin_no) as mf_count from asset_pledge_data group by application_id) apd
                    on apd.application_id = x.application_id) app_data
                    where app_data.app_pan is not null
                    """, conn) 

        print("application_data exectued()...")
        email_otp_validation= pd.read_sql_query("""
                                select cmr2.* from
                                (select max(record_last_updated_on) as record_last_updated_on, primary_recipient
                                    from communication_message_records 
                                        where message_type= 'EMAIL'
                                        group by primary_recipient) cmr1
                                inner join
                                (select message_type, primary_recipient, record_created_on, record_last_updated_on, status as email_status 
                                        from communication_message_records where message_type= 'EMAIL') cmr2
                                    on cmr1.primary_recipient= cmr2.primary_recipient
                                    and cmr1.record_last_updated_on= cmr2.record_last_updated_on
                                    """, conn)
        print("email_otp_validations")
        conn.close()

    print("data reading checked() ...")
    lpd_cl = pd.merge(customer_leads[['lead_id', 'pan']], leads_portfolio_data[['lead_id', 'created_on']], on='lead_id')
    lpd_cl_groupby = lpd_cl.groupby('pan').agg({'created_on':'min'}).reset_index()
    
    vlq_cl = pd.merge(left=customer_leads, right= leads_portfolio_data, on='lead_id', how='left')
    vlq_cl_merged = pd.merge(left=vlq_cl, right= lpd_cl_groupby[['pan', 'created_on']], on=['pan', 'created_on'], how = 'right')
    
    vlq_cl_merged= pd.merge(vlq_cl_merged, email_otp_validation, left_on= ['email_id'], right_on= ['primary_recipient'], how= 'left')

    application_data = application_data.reset_index(drop= True)
    final_df = pd.merge(left=application_data, right=vlq_cl_merged, left_on='app_pan', right_on='pan', how='outer')
 
# ------------------------------------------------------------------------------------------------------------------------ 
    # collate all values():
    final_df['platform_account_id'] = final_df['platform_account_id_x'].combine_first(final_df['platform_account_id_y'])
    final_df['platform_name'] = final_df['platform_name_x'].combine_first(final_df['platform_name_y'])
    final_df['partner_account_id'] = final_df['partner_account_id_x'].combine_first(final_df['partner_account_id_y'])
    final_df['partner_account_type']= final_df['partner_account_type_x'].combine_first(final_df['partner_account_type_y'])
    final_df['partner_name']= final_df['partner_name_x'].combine_first(final_df['partner_name_y'])
    final_df['account_holder_name']= final_df['full_name'].combine_first(final_df['account_holder_name'])
    final_df['pan'] = final_df['pan'].combine_first(final_df['app_pan'])
    final_df['phone_number']= final_df['app_phone_number'].combine_first(final_df['mfc_phone_number'])
    final_df['last_updated_date'] = final_df['last_updated_on'].combine_first(final_df['app_last_updated_on'])
    final_df['email_address']= final_df['email_id'].combine_first(final_df['account_holder_email'])

    # get created_on of the application():
    final_df['created_on']= np.where(((final_df['platform_account_id_x'] == final_df['platform_account_id_y']) & \
        (final_df['app_created_on']<final_df['mfc_created_on'])) | final_df['mfc_created_on'].isna(), final_df['app_created_on'], final_df['mfc_created_on'])

    final_df= final_df.drop(columns = ['partner_account_id_x', 'partner_account_id_y', 'partner_name_y', 'partner_name_x'])

    # platform_y = "mfc_platform" & platform_x= "app_platform", : phonepe platorm_data():
    final_df = final_df[(final_df['platform_account_id_x'] == '602f3c59-b135-49cb-bb14-d0c2ba9a13f8') | \
                (final_df['platform_account_id_y'] == '602f3c59-b135-49cb-bb14-d0c2ba9a13f8')] 
    
    # Convert back to datetime before adding timedelta
    final_df['created_on'] = pd.to_datetime(final_df['created_on']) + timedelta(hours=5, minutes=30)
    final_df['completed_on'] = pd.to_datetime(final_df['completed_on']) + timedelta(hours=5, minutes=30)
    final_df['last_updated_date'] = pd.to_datetime(final_df['last_updated_date']) + timedelta(hours=5, minutes=30)

    # Convert to datetime and format as date string
    final_df['last_updated_date'] = pd.to_datetime(final_df['last_updated_date']).dt.strftime('%Y-%m-%d')
    final_df['created_on'] = pd.to_datetime(final_df['created_on']).dt.strftime('%Y-%m-%d')
    final_df['completed_on'] = pd.to_datetime(final_df['completed_on']).dt.strftime('%Y-%m-%d')

    # Extract date part
    final_df['created_on'] = pd.to_datetime(final_df['created_on']).dt.date
    final_df['completed_on'] = pd.to_datetime(final_df['completed_on']).dt.date
    
    # Define start and end dates
    start_date = pd.to_datetime('2024-05-01').date()
    final_df = final_df[(final_df['created_on'] >= start_date) | (final_df['completed_on'] >= start_date)]

    final_df['current_step_id']= np.where((final_df['application_id'].isnull()) & (final_df['current_step_id'].isnull()), "MFC Fetched", final_df['current_step_id'])

    final_df.replace({pd.NaT: None}, inplace=True)
    final_df.fillna('', inplace=True)

    final_df.rename(columns = {'pan': 'account_holderpan', 
                                'phone_number': 'account_holder_phone_number', 
                                'platform_name_y': 'mfc_platform', 
                                'platform_name_x': "app_platform"}, inplace= True)

    final_df= final_df[['account_holderpan', 'account_holder_phone_number', 'application_id', 'created_on', 'current_step_id', 'completed_on', 'cams_fetched', 
                        'karvy_fetched', 'mfc_eligible_credit_limit', 'app_eligible_credit_limit', 'app_pledged_credit_limit', 'email_address', 'mf_count', 
                        'request_id', 'platform_name', 'mfc_created_on', 'app_created_on', 'email_status']]

    print("dumping_started .... ")
    final_df.to_csv("final_df.csv", index= False)

    client = pygsheets.authorize(service_account_file='/home/ec2-user/configurations_dir/dataautomation.json')
    client = pygsheets.authorize(service_account_file='/Users/puneet/Code/configurations_dir/dataautomation.json')

    print("filed_dumping started() ...")
    SPREADSHEET_ID = "1RNoHv8XF3cZxha45dbx8Rl1MZxi5NUyBUvOdaACzx3I"
    sh = client.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet("title", "created_on cases")
    
    ws.resize(final_df.shape[0], final_df.shape[1])
    ws.clear(start='A1', end=None)
    ws.set_dataframe(final_df, start=(1,1))

if __name__ == "__main__":
    get_db_data()