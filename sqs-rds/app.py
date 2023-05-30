import boto3
import mysql.connector
import json
import time
import configparser

from flask import Flask, render_template

app = Flask(__name__)

# AWS SQS configuration
region = 'us-east-2'  # Replace with your AWS region (e.g., 'us-west-2')
queue_url = 'https://sqs.us-east-2.amazonaws.com/778168509800/python'  # Replace with your SQS queue URL

# Read MySQL configuration from properties file
def read_mysql_config():
    config = configparser.ConfigParser()
    config.read('config.properties')

    db_host = config.get('MySQL', 'db.host')
    db_user = config.get('MySQL', 'db.username')
    db_password = config.get('MySQL', 'db.password')
    db_name = config.get('MySQL', 'db.name')
    db_table = config.get('MySQL', 'db.table')

    return db_host, db_user, db_password, db_name, db_table

# Create an SQS client
sqs = boto3.client('sqs', region_name=region)

# Create a MySQL connection
db_host, db_user, db_password, db_name, db_table = read_mysql_config()
mysql_conn = mysql.connector.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database=db_name
)

# Create the MySQL table if it doesn't exist
cursor = mysql_conn.cursor()
create_table_query = f"CREATE TABLE IF NOT EXISTS {db_table} (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255), additional_data VARCHAR(255))"
cursor.execute(create_table_query)
mysql_conn.commit()
cursor.close()

# Function to process SQS messages and store in MySQL
def process_sqs_messages(queue_url):
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10
    )

    messages = response.get('Messages', [])

    for message in messages:
        body = message['Body']
        data = json.loads(body)

        name = data['name']
        email = data['email']
        additional_data = data['additionalData']

        cursor = mysql_conn.cursor()
        insert_query = f"INSERT INTO {db_table} (name, email, additional_data) VALUES (%s, %s, %s)"
        cursor.execute(insert_query, (name, email, additional_data))
        mysql_conn.commit()
        cursor.close()

        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message['ReceiptHandle']
        )

# Continuously fetch and process messages from SQS
def start_message_processing():
    while True:
        process_sqs_messages(queue_url)
        time.sleep(5)

# View data route
@app.route('/view_data')
def view_data():
    cursor = mysql_conn.cursor()
    select_query = f"SELECT * FROM {db_table}"
    cursor.execute(select_query)
    data = cursor.fetchall()
    cursor.close()

    return render_template('view_data.html', data=data)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)
