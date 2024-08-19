import psycopg2
from sshtunnel import SSHTunnelForwarder
import pygsheets
import pandas as pd
from datetime import date, timedelta, datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email import encoders
from io import StringIO
import collection_email
from datetime import date, timedelta, datetime

def email_data(dataframe):
    yesterday = datetime.today() - timedelta(days=1)
    yesterday_date = yesterday.date()
    formatted_date = yesterday_date.strftime("%d %b %Y")

## credentials --
    SMTP_SERVER = 'smtp.gmail.com'
    SMTP_PORT = 587
    SMTP_USERNAME = 'puneet.pushkar@voltmoney.in'
    SMTP_PASSWORD = 'kbxg hzls kdty fnoe'

## emails --
    FROM_EMAIL = 'puneet.pushkar@voltmoney.in'
    TO_EMAIL = ['operations@voltmoney.in'] 
    CC_EMAIL = ['puneet.pushkar@voltmoney.in']  
    BCC_EMAIL = ['puneet.pushkar@voltmoney.in']  

    msg = MIMEMultipart()
    msg['From'] = FROM_EMAIL
    msg['To'] = ', '.join(TO_EMAIL)
    
    if CC_EMAIL:
        msg['CC'] = ', '.join(CC_EMAIL)
    if BCC_EMAIL:
        msg['BCC'] = ', '.join(BCC_EMAIL)

    msg['Subject'] = f"Volt accounts - Repayments - {formatted_date}"    
    signature= "Regards,\nOperations Volt Money"

    # body:
    body = f"Hi, \nPlease find attached the list of repayments done on {formatted_date}"
    msg.attach(MIMEText(body + '\n\n'))

    # get the values as a text format method():
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    part = MIMEBase('text', 'csv')
    part.set_payload(csv_buffer.getvalue())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename="data.csv"')

    msg.attach(part)

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        # server.ehlo()
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(msg['From'], TO_EMAIL + CC_EMAIL + BCC_EMAIL, msg.as_string())
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print("Email could not be sent. Error:", str(e))

def main():
    dataframe= collection_email.main()
    final_df= email_data(dataframe)


if __name__ == '__main__':
    main()
