"""
Spark 배치 처리 - 수집된 JSON 파일 분석
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, hour, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def create_spark_session() -> SparkSession:
    """SparkSession 생성"""
    # TODO: SparkSession 빌더 패턴으로 생성
    spark = SparkSession.builder \
        .appName("MetroBatchAnalysis") \
        .master("local[*]") \
        .getOrCreate()

    # 로그 레벨 설정 (INFO 메시지 줄이기)
    spark.sparkContext.setLogLevel("WARN")

    return spark


def load_json_files(spark: SparkSession, path: str):
    """JSON 파일들을 DataFrame으로 로드"""

    # 스키마 정의 (옵션 - 자동 추론도 가능)
    schema = StructType([
        StructField("station_name", StringType(), True),
        StructField("subway_id", StringType(), True),
        StructField("train_line", StringType(), True),
        StructField("arrival_message", StringType(), True),
        StructField("arrival_time_sec", StringType(), True),
        StructField("received_at", StringType(), True),
        StructField("collected_at", StringType(), True),
    ])

    # TODO: JSON 파일 읽기 (multiLine=True, 배열 형식 JSON 지원)
    # Kafka로 저장한 파일은 messages 배열 안에 데이터가 있음
    raw_df = spark.read \
        .option("multiLine", True) \
        .json(path)

    # messages 배열 펼치기 (explode)
    from pyspark.sql.functions import explode

    # TODO: messages 배열을 행으로 펼치기
    df = raw_df.select(explode(col("messages")).alias("msg"))

    # 중첩 구조 펼치기
    df = df.select(
        col("msg.station_name").alias("station_name"),
        col("msg.subway_id").alias("subway_id"),
        col("msg.train_line").alias("train_line"),
        col("msg.arrival_message").alias("arrival_message"),
        col("msg.arrival_time_sec").cast(IntegerType()).alias("arrival_time_sec"),
        col("msg.received_at").alias("received_at"),
        col("msg.collected_at").alias("collected_at"),
    )

    return df


def analyze_by_station(df):
    """역별 도착 정보 집계"""
    # TODO: station_name으로 그룹화하고 개수 세기
    station_stats = df.groupBy("station_name") \
        .agg(
            count("*").alias("arrival_count"),
            avg("arrival_time_sec").alias("avg_wait_sec")
        ) \
        .orderBy(col("arrival_count").desc())

    return station_stats


def analyze_by_line(df):
    """호선별 집계"""
    # 호선 코드 → 호선명 매핑 딕셔너리
    # API 응답의 subwayId는 문자열 코드 형태 (예: "1002" = 2호선)
    line_mapping = {
        "1001": "1호선", "1002": "2호선", "1003": "3호선",
        "1004": "4호선", "1005": "5호선", "1006": "6호선",
        "1007": "7호선", "1008": "8호선", "1009": "9호선",
    }

    from pyspark.sql.functions import udf

    # UDF 정의: Python 딕셔너리로 매핑하는 커스텀 함수
    # @udf(반환타입) 데코레이터로 Spark UDF로 등록
    @udf(StringType())
    def get_line_name(subway_id):
        """subway_id(예: "1002")를 호선명(예: "2호선")으로 변환"""
        return line_mapping.get(subway_id, "기타")

    # UDF를 컬럼에 적용하여 새 컬럼 생성
    # withColumn(새컬럼명, 변환함수(기존컬럼))
    df_with_line = df.withColumn("line_name", get_line_name(col("subway_id")))

    # TODO: line_name으로 그룹화하고 집계
    line_stats = df_with_line.groupBy("line_name") \
        .agg(
            count("*").alias("total_count"),
            avg("arrival_time_sec").alias("avg_wait_sec")
        ) \
        .orderBy(col("total_count").desc())

    return line_stats


def save_to_parquet(df, path: str):
    """DataFrame을 Parquet 형식으로 저장"""
    # TODO: Parquet 형식으로 저장 (덮어쓰기 모드)
    df.write \
        .mode("overwrite") \
        .parquet(path)

    print(f"저장 완료: {path}")


if __name__ == "__main__":
    # SparkSession 생성
    spark = create_spark_session()

    print("=" * 60)
    print("Spark 배치 처리 시작")
    print("=" * 60)

    # 데이터 로드
    df = load_json_files(spark, "/data/kafka_batch_*.json")

    print(f"\n전체 레코드 수: {df.count()}")
    print("\n데이터 샘플:")
    df.show(5, truncate=False)

    # 역별 분석
    print("\n" + "=" * 60)
    print("역별 도착 정보 집계")
    print("=" * 60)
    station_stats = analyze_by_station(df)
    station_stats.show(10)

    # 호선별 분석
    print("\n" + "=" * 60)
    print("호선별 집계")
    print("=" * 60)
    line_stats = analyze_by_line(df)
    line_stats.show()

    # 결과 저장
    save_to_parquet(station_stats, "/output/station_stats")
    save_to_parquet(line_stats, "/output/line_stats")

    # SparkSession 종료
    spark.stop()
    print("\nSpark 배치 처리 완료")