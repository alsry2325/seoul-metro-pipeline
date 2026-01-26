"""
Kafka Producer - 지하철 도착 정보를 Kafka로 전송
"""
import os
import json
import time
from datetime import datetime
from confluent_kafka import Producer
from api_client import SeoulMetroAPI


# 수집할 역 목록 (API 일일 1,000건 제한 고려)
# ⚠️ 주의: 스트림 처리 시 역 수 × 시간당 실행 횟수를 반드시 계산하세요!
# 예: 3개 역 × 6회/시간(10분 간격) × 24시간 = 432건/일 ✅
# ⚠️ 중요: 서울역은 "서울"로 검색해야 함 ("서울역" ❌)
TARGET_STATIONS = ["강남", "홍대입구", "서울"]  # 3개 역으로 제한

# Kafka 설정
KAFKA_TOPIC = "metro-arrivals"

# API 호출 제한 설정
DEFAULT_INTERVAL_SECONDS = 600  # 10분 간격 (권장: 일일 432건)


def create_producer() -> Producer:
    """Kafka Producer 생성"""
    # TODO: 환경변수에서 Kafka 서버 주소 가져오기
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    config = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "metro-producer",
    }

    return Producer(config)


def delivery_callback(err, msg):
    """메시지 전송 결과 콜백"""
    if err:
        print(f"❌ 전송 실패: {err}")
    else:
        print(f"✅ 전송 성공: {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def produce_arrivals(producer: Producer, stations: list = None):
    """
    역 도착 정보를 Kafka로 전송
    """
    if stations is None:
        stations = TARGET_STATIONS

    api = SeoulMetroAPI()
    collected_at = datetime.now().isoformat()
    message_count = 0

    for station in stations:
        print(f"수집 중: {station}...")
        result = api.get_arrival_info(station)

        if "realtimeArrivalList" not in result:
            continue

        for arrival in result["realtimeArrivalList"]:
            # 메시지 생성
            message = {
                "station_name": arrival.get("statnNm"),
                "subway_id": arrival.get("subwayId"),
                "train_line": arrival.get("trainLineNm"),
                "arrival_message": arrival.get("arvlMsg2"),
                "arrival_time_sec": arrival.get("barvlDt"),
                "received_at": arrival.get("recptnDt"),
                "collected_at": collected_at
            }

            # TODO: 메시지를 JSON으로 직렬화하여 Kafka로 전송
            # producer.produce(토픽, 값, 콜백)
            producer.produce(
                KAFKA_TOPIC,
                json.dumps(message).encode("utf-8"),
                callback=delivery_callback
            )

            message_count += 1

        # 배치 전송 (버퍼 비우기)
        producer.poll(0)

    # 모든 메시지 전송 완료 대기
    # TODO: 버퍼에 남은 메시지 모두 전송
    producer.flush()

    return message_count


def run_continuous(interval_seconds: int = 600):
    """
    지속적으로 데이터 수집 및 전송 (스트림 처리 시뮬레이션)

    Args:
        interval_seconds: 수집 간격 (기본 600초=10분)
                         ⚠️ API 일일 1,000건 제한 고려!
                         - 60초(1분) 간격: 3개역 × 1440회 = 4,320건/일 ❌ 초과
                         - 300초(5분) 간격: 3개역 × 288회 = 864건/일 ⚠️ 주의
                         - 600초(10분) 간격: 3개역 × 144회 = 432건/일 ✅ 권장
    """
    producer = create_producer()
    print(f"Producer 시작 - {interval_seconds}초 간격으로 수집")
    print(f"⚠️ API 호출량 예상: {len(TARGET_STATIONS)}개 역 × {86400//interval_seconds}회 = {len(TARGET_STATIONS) * (86400//interval_seconds)}건/일")

    try:
        while True:
            print(f"\n[{datetime.now().isoformat()}] 수집 시작...")
            count = produce_arrivals(producer)
            print(f"전송 완료: {count}개 메시지")

            print(f"{interval_seconds}초 대기...")
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\n종료 요청...")
    finally:
        producer.flush()
        print("Producer 종료")


if __name__ == "__main__":
    # 단일 실행 모드
    producer = create_producer()
    count = produce_arrivals(producer)
    print(f"\n총 {count}개 메시지 전송 완료")

    # 지속 실행 모드 (주석 해제하여 사용)
    # ⚠️ API 일일 1,000건 제한! 600초(10분) 이상 간격 권장
    # run_continuous(interval_seconds=600)  # 10분 간격: 432건/일