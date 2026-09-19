import time
import os
import boto3 #deal with s3 bucket
import requests
import json
from kafka import KafkaConsumer


#minio connection
s3=boto3.client(
    "s3",
    endpoint_url="http://localhost:9002",
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")


)

bucket_name="bronze-transactions"


#define consumer
consumer=KafkaConsumer(
    "stock_quotes",
    bootstrap_servers=["localhost:29092"],
    enable_auto_commit=True,
    group_id="bronze-consumer-new",
    auto_offset_reset="earliest",
    value_deserializer=lambda v:json.loads(v.decode("utf-8"))
)


print("saving to minio")

#main function
for message in consumer:
    record=message.value
    symbol=record.get("symbol","unknown")
    ts=record.get("fetched_at",int(time.time()))
    key=f"{symbol}/{ts}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(record),
        ContentType="application/json"

    )

    print(f"saved record {symbol} = s3 bucket={bucket_name}/{key}")
