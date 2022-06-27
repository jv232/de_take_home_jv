import json
import localstack_client.session as boto3
import pandas as pd
import psycopg2
from datetime import datetime, timezone

QUEUE_NAME = "login-queue"
COLUMNS = [
    'user_id', 'app_version', 'device_type', 'ip', 'locale', 'device_id']
MAX_MESSAGE_NUMBER = 100


def get_messages():
    sqs = boto3.client("sqs")

    try:
        # get sqs queue url
        queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
        # receives message from sqs with attribute name "messages"
        queue = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=MAX_MESSAGE_NUMBER, AttributeNames=['Messages'])
    except Exception as error:
        print("Run the docker", error)
        return

    message_list = []

    # turns json to dict to list
    for message in queue["Messages"]:
        data = message["Body"]
        data = json.loads(data)
        message_list.append(list(data.values()))

    return message_list


def mask_data(messageList):

    df = pd.DataFrame(messageList, columns=COLUMNS)

    # the values are hashed
    # if the values are the same hashed value will be the same
    df['ip'] = pd.util.hash_array(df['ip'].to_numpy())
    df['device_id'] = pd.util.hash_array(df['device_id'].to_numpy())

    return df


def database(df):
    dt = datetime.now(timezone.utc)
    try:
        conn = psycopg2.connect(database="postgres",
                                user="postgres", password="postgres")
        conn.autocommit = True
        cursor = conn.cursor()

        # creating a table if not already exists
        user_logins_table = (''' CREATE TABLE IF NOT EXISTS 
        user_logins (
            user_id             varchar(128),
            device_type         varchar(32),
            masked_ip           varchar(256),
            masked_device_id    varchar(256),
            locale              varchar(32),
            app_version         varchar(32),
            create_date         date
            );
        ''')
        cursor.execute(user_logins_table)

        for i in range(len(df)):
            insert_query = (
                ''' INSERT INTO user_logins (USER_ID, DEVICE_TYPE, MASKED_IP, MASKED_DEVICE_ID, LOCALE, APP_VERSION, CREATE_DATE) VALUES (%s,%s,%s,%s, %s, %s, %s) ''')

            # replacing "." in app_version to "0" (zero)
            record_insert = (df['user_id'][i], df['device_type'][i], str(df['ip'][i]), str(
                df['device_id'][i]), df['locale'][i], int(df['app_version'][i].replace(".", "0")), dt)

            cursor.execute(insert_query, record_insert)

        # cursor.execute(''' SELECT * FROM user_logins''')

        # Fetching all row from the table
        # result = cursor.fetchall()
        # print(result)

        # Commit your changes in the database
        conn.commit()

        # Closing the connection
        cursor.close()
        conn.close()
    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into user_logins", error)
        return

    return


def main():
    message_list = get_messages()
    modified_message_list = mask_data(message_list)
    database(modified_message_list)


if __name__ == "__main__":
    main()
