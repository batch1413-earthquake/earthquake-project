from google.cloud import pubsub_v1
from google.cloud import bigquery
from fastapi import FastAPI
from pydantic import BaseModel
import json


project_id = "batch1413-earthquake"
topic_id = "earthquake-messaging-system"
subscription_name = "earthquake-messaging-system-sub"
client = bigquery.Client()
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)
dataset_id = "gold_earthquake_dataset"
table_earth = "earthquakes"
table_user = "random_users"
table_earthquake = f"{project_id}.{dataset_id}.{table_earth}"
table_user = f"{project_id}.{dataset_id}.{table_user}"

app = FastAPI()

messages_to_ack = []


@app.get("/")
async def read_root():
    try:
        data = queryEarthquake()
        id = data[0]
        print(id)
        listUsers = getUserImpacted(id)
        print("How many users are impacted ? " + str(len(listUsers)))
        message = "New earthquake alert : users impacted " + str(len(listUsers))
        publish_message(message)
        return {"message send to Pub/Sub": message}
    except:
        return {"error": "can not perform queries"}


def queryEarthquake():
    sql = f"SELECT * FROM `{table_earthquake}` ORDER BY RAND() LIMIT 1;"
    query_job = client.query(sql)
    return list(query_job.result())[0]


def getUserImpacted(idEarthquake):
    sql = f"""
        WITH event_and_users AS (
            SELECT
                ST_GEOGPOINT(
                (SELECT longitude  FROM `{table_earthquake}` WHERE id = '{idEarthquake}'),
                (SELECT latitude  FROM `{table_earthquake}` WHERE id = '{idEarthquake}')
                ) AS event_loc,
                ST_GEOGPOINT(longitude, latitude) AS user_loc,
            FROM `{table_user}`
            )
            SELECT *
            FROM (
            SELECT
                ST_DISTANCE(event_loc, user_loc) AS distance,
                *
            FROM event_and_users
            )
            WHERE (distance/1000) < 250
    """
    query_job = client.query(sql)
    return list(query_job.result())


def publish_message(data):
    """Publishes a message to a Pub/Sub topic."""
    data_str = json.dumps(data)
    data_bytes = data_str.encode("utf-8")
    future = publisher.publish(topic_path, data=data_bytes)
    print(f"Published message ID: {future.result()}")


if __name__ == "__main__":
    data = queryEarthquake()
    id = data[0]
    print(id)
    listUsers = getUserImpacted(id)
    print("How many users are impacted ? " + str(len(listUsers)))
    message = "New earthquake alert : users impacted " + str(len(listUsers))
    message = {"message": message}
    publish_message(message)
