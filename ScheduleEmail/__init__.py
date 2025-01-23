import os
import subprocess
import sys

# Install dependencies
subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "./requirements.txt"])
import os
import sqlalchemy
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from sqlalchemy.sql import text
#from azure.cosmos import CosmosClient, PartitionKey, exceptions
import urllib
import azure.functions as func
import logging
import datetime

def get_db_connection():
    db_connection_string = os.getenv('DB_URI')
    if not db_connection_string:
        raise ValueError("DB_URI environment variable not set.")

    try:
        params = urllib.parse.quote_plus(db_connection_string)
        sqlalchemy_url = f"mysql+pyodbc:///?odbc_connect={params}"
        engine = sqlalchemy.create_engine(sqlalchemy_url, pool_size=5, pool_timeout=30, pool_recycle=1800)
        conn = engine.connect()
        print("Connected to the database.")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise

# def get_cosmos_client():
#     cosmos_endpoint = os.getenv('COSMOS_ENDPOINT')
#     cosmos_key = os.getenv('COSMOS_KEY')
#     cosmos_database = os.getenv('COSMOS_DATABASE')
#     cosmos_container = os.getenv('COSMOS_CONTAINER')

#     if not all([cosmos_endpoint, cosmos_key, cosmos_database, cosmos_container]):
#         raise ValueError("Cosmos DB environment variables not set.")

#     client = CosmosClient(cosmos_endpoint, cosmos_key)
#     database = client.get_database_client(cosmos_database)
#     container = database.get_container_client(cosmos_container)
#     return container

def fetch_users_and_balances(conn):
    query = text("""
        SELECT
            u.username AS nick,
            u.email_address AS email,
            STRING_AGG(CONCAT(c.currency_code, ': ', c.sum_amount), ', ') AS waluty_zsumowane
        FROM user u
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

# def save_summary_to_cosmosdb(container, username, email, balances_summary):
#     try:
#         document = {
#             "id": email,
#             "username": username,
#             "email": email,
#             "balances_summary": balances_summary
#         }
#         container.upsert_item(document)
#         print(f"Saved summary for {email} to Cosmos DB.")
#     except exceptions.CosmosHttpResponseError as e:
#         print(f"Failed to save summary to Cosmos DB: {str(e)}")

def send_email(sendgrid_client, to_email, amount_summary):
    message = Mail(
        from_email=os.getenv('SENDGRID_EMAIL'),
        to_emails=to_email,
        subject='Your Account Balance',
        html_content=f"<p>Your current balances: {amount_summary}</p>"
    )
    response = sendgrid_client.send(message)
    return response.status_code

def main(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    sendgrid_api_key = os.getenv('SENDGRID_API_KEY')
    if not sendgrid_api_key:
        print("SENDGRID_API_KEY not set.")

    sendgrid_client = SendGridAPIClient(sendgrid_api_key)
    conn = None
    cosmos_container = None
    try:
        conn = get_db_connection()
        #cosmos_container = get_cosmos_client()
        users = fetch_users_and_balances(conn)
        print(f"Found {len(users)} users.")
        if not users:
            print("No users found.")
            return ("No users to send emails to.", 200)

        for username, email, amount_summary in users:

            status = send_email(sendgrid_client, email, amount_summary)
            if status != 202:
                print(f"Failed to send email to {email}, status code: {status}")
            print(f"Sent email to {email}.")
            #save_summary_to_cosmosdb(cosmos_container, username, email, amount_summary)
    except Exception as e:
        print(f"Error in main function: {str(e)}")
    finally:
        if conn:
            conn.close()
