"""
Spark Structured Streaming - Kafka에서 실시간 데이터 처리
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, avg,
    current_timestamp, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)


# Kafka 설정
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "metro-arrivals"


def create_spark_session() -> SparkSession:
    """Streaming용 SparkSession 생성"""
    spark = SparkSession.builder \
        .appName("MetroStreaming") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_message_schema():
    """Kafka 메시지의 JSON 스키마"""
    return StructType([
        StructField("station_name", StringType(), True),
        StructField("subway_id", StringType(), True),
        StructField("train_line", StringType(), True),
        StructField("arrival_message", StringType(), True),
        StructField("arrival_time_sec", StringType(), True),
        StructField("received_at", StringType(), True),
        StructField("collected_at", StringType(), True),
    ])


def read_from_kafka(spark: SparkSession):
    """Kafka에서 스트림 읽기"""

    # TODO: Kafka 소스에서 스트림 읽기
    # format: "kafka"
    # option: kafka.bootstrap.servers, subscribe
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    return kafka_df


def parse_messages(kafka_df, schema):
    """Kafka 메시지 파싱"""

    # Kafka 메시지의 value는 바이트 배열이므로 문자열로 변환
    # TODO: value 컬럼을 문자열로 캐스팅
    string_df = kafka_df.selectExpr("CAST(value AS STRING) as value")

    # TODO: JSON 파싱 (from_json 함수 사용)
    parsed_df = string_df.select(
        from_json(col("value"), schema).alias("data")
    )

    # 중첩 구조 펼치기
    flat_df = parsed_df.select(
        col("data.station_name").alias("station_name"),
        col("data.subway_id").alias("subway_id"),
        col("data.train_line").alias("train_line"),
        col("data.arrival_message").alias("arrival_message"),
        col("data.arrival_time_sec").cast(IntegerType()).alias("arrival_time_sec"),
        to_timestamp(col("data.collected_at")).alias("event_time"),
    ).withColumn("processing_time", current_timestamp())

    return flat_df


def aggregate_by_station(df):
    """역별 실시간 집계 (1분 윈도우)"""

    # TODO: 1분 윈도우로 역별 집계
    # window(이벤트시간컬럼, 윈도우크기)
    aggregated = df \
        .withWatermark("event_time", "1 minute") \
        .groupBy(
            window("event_time", "1 minute"),
            "station_name"
        ) \
        .agg(
            count("*").alias("arrival_count"),
            avg("arrival_time_sec").alias("avg_wait_sec")
        )

    return aggregated


def write_to_console(df, output_mode="complete"):
    """콘솔로 출력 (디버깅용)"""

    # TODO: 콘솔 싱크로 스트림 쓰기
    query = df.writeStream \
        .format("console") \
        .outputMode(output_mode) \
        .option("truncate", False) \
        .start()

    return query


def write_to_parquet(df, path: str):
    """Parquet 파일로 저장 (append 모드)"""

    query = df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", path) \
        .option("checkpointLocation", f"{path}/_checkpoint") \
        .start()

    return query


if __name__ == "__main__":
    print("=" * 60)
    print("Spark Structured Streaming 시작")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print("=" * 60)

    # SparkSession 생성
    spark = create_spark_session()

    # 스키마 정의
    schema = get_message_schema()

    # Kafka에서 읽기
    kafka_df = read_from_kafka(spark)

    # 메시지 파싱
    parsed_df = parse_messages(kafka_df, schema)

    # 역별 집계
    station_agg = aggregate_by_station(parsed_df)

    # 콘솔 출력 (디버깅)
    console_query = write_to_console(station_agg, "complete")

    # Parquet 저장 (원본 데이터)
    # parquet_query = write_to_parquet(parsed_df, "output/streaming_data")

    print("\nStreaming 시작... (Ctrl+C로 종료)")

    # 스트림 대기
    console_query.awaitTermination()