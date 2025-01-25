import os
import subprocess
import sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "./requirements.txt"])
import sqlalchemy
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from sqlalchemy.sql import text
import urllib
import azure.functions as func
import logging
import datetime
import uuid

def get_db_connection():
    db_connection_string = os.getenv('DB_URI')
    if not db_connection_string:
        raise ValueError("DB_URI environment variable not set.")

    try:
        params = urllib.parse.quote_plus(db_connection_string)
        sqlalchemy_url = "mssql+pyodbc:///?odbc_connect=%s" % params
        engine = sqlalchemy.create_engine(sqlalchemy_url, pool_size=5, pool_timeout=30, pool_recycle=1800)
        conn = engine.connect()
        logging.info("Connected to the database.")
        return conn
    except Exception as e:
        logging.info(f"Error connecting to the database: {e}")
        raise

def fetch_users_and_balances(conn):
    query = text("""
        SELECT
            u.username AS nick,
            u.email_address AS email,
            STRING_AGG(CONCAT(c.currency_code, ': ', c.sum_amount), ', ') AS waluty_zsumowane
        FROM [user] u
        JOIN (
            SELECT w.user_id, w.currency_code, SUM(w.amount) AS sum_amount
            FROM wallet w
            GROUP BY w.user_id, w.currency_code
        ) c ON u.id = c.user_id
        GROUP BY u.id, u.username, u.email_address;
        """)

    result = conn.execute(query)
    rows = result.mappings().all()
    return [(row['email'], row['waluty_zsumowane']) for row in rows]

def send_email(sendgrid_client, to_email, amount_summary):
    message = Mail(
        from_email=os.getenv('SENDGRID_EMAIL'),
        to_emails=to_email,
        subject='Your Account Balance',
        html_content=f"<p>Your current balances: {amount_summary}</p>"
    )
    response = sendgrid_client.send(message)
    return response.status_code

def main(myTimer: func.TimerRequest, doc: func.Out[func.Document]) -> None:
    utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    sendgrid_api_key = os.getenv('SENDGRID_API_KEY')
    if not sendgrid_api_key:
        logging.info("SENDGRID_API_KEY not set.")

    sendgrid_client = SendGridAPIClient(sendgrid_api_key)
    conn = None
    try:
        conn = get_db_connection()
        users = fetch_users_and_balances(conn)
        logging.info(f"Found {len(users)} users.")
        if not users:
            logging.info("No users found.")
            return ("No users to send emails to.", 200)

        for email, amount_summary in users:
            status = send_email(sendgrid_client, email, amount_summary)
            if status != 202:
                logging.info(f"Failed to send email to {email}, status code: {status}")
            logging.info(f"Sent email to {email}.")
            document = {
                "id": str(uuid.uuid4()),
                "email": email,
                "balances_summary": amount_summary,
                "timestamp": utc_timestamp
            }
            doc.set(func.Document.from_dict(document))
            logging.info(f"CosmosDB document created for {email}.")
    except Exception as e:
        logging.info(f"Error in main function: {str(e)}")
    finally:
        if conn:
            conn.close()
