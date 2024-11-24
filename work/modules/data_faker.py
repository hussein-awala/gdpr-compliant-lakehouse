import uuid
from datetime import timedelta, datetime
import random

from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.types import StructType, StringType, TimestampType, StructField


def _generate_users_data(num_records: int) -> list[Row]:
    data = []
    for _ in range(num_records):
        user = Row(
            user_id=str(uuid.uuid4()),
            created_at=(
                datetime.now()
                - timedelta(days=random.randint(1, 100))
                - timedelta(seconds=random.randint(1, 60 * 60 * 24))
            ),
            name=f"User{random.randint(1, num_records * 1000)}",
            email=f"user{random.randint(1, num_records * 1000)}@example.com"
        )
        data.append(user)
    return data


def generate_users_dataframe(spark_session: SparkSession, num_records: int) -> DataFrame:
    user_data = _generate_users_data(num_records)
    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("name", StringType(), False),
        StructField("email", StringType(), False),
    ])
    return spark_session.createDataFrame(user_data, schema)


def _generate_events_data(users_df: DataFrame, num_events: int) -> list[Row]:
    event_types = ["page_view", "click", "purchase"]
    page_urls = ["/home", "/product", "/cart", "/checkout"]

    users_data = users_df.collect()
    events_data = []
    for _ in range(num_events):
        user = random.choice(users_data)  # Randomly select a user
        event_type = random.choice(event_types)
        event_details = ""
        page_url = None
        product_id = None

        # Simulate different event types with corresponding details
        if event_type == "page_view":
            page_url = random.choice(page_urls)
            event_details = f"Viewed {page_url}"
        elif event_type == "click":
            page_url = random.choice(page_urls)
            event_details = f"Clicked on {page_url}"
        elif event_type == "purchase":
            product_id = f"product_{random.randint(1, 100)}"
            event_details = f"Purchased {product_id}"

        event = Row(
            event_id=str(uuid.uuid4()),
            user_id=user.user_id,
            event_type=event_type,
            event_timestamp=(
                datetime.now()
                - timedelta(days=random.randint(1, 100))
                - timedelta(seconds=random.randint(1, 60 * 60 * 24))
            ),
            page_url=page_url,
            product_id=product_id,
            event_details=event_details
        )
        events_data.append(event)

    return events_data


def generate_events_dataframe(spark_session: SparkSession, users_df: DataFrame, num_events: int) -> DataFrame:
    events_data = _generate_events_data(users_df, num_events)

    # Define the schema for the events table
    events_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("page_url", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("event_details", StringType(), True)
    ])

    # Create DataFrame
    return spark_session.createDataFrame(events_data, events_schema)


def generate_gdpr_request_dataframe(spark_session: SparkSession, users_df: DataFrame, delete_count: int, export_count: int) -> DataFrame:
    gdpr_requests = [
        Row(user_id=r.user_id, request_type="delete") for r in (
            users_df
                .select("user_id")
                .rdd.takeSample(False, delete_count)
        )
    ] + [
        Row(user_id=r.user_id, request_type="export") for r in (
            users_df
                .select("user_id")
                .rdd.takeSample(False, export_count)
        )
    ]
    return spark_session.createDataFrame(
        gdpr_requests,
        StructType([
            StructField("user_id", StringType(), False),
            StructField("request_type", StringType(), False)
        ])
    )
