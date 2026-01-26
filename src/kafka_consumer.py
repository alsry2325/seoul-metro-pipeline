"""
Kafka Consumer - 지하철 도착 정보를 소비하여 저장
"""
import os
import json
from datetime import datetime
from pathlib import Path
from confluent_kafka import Consumer, KafkaException


KAFKA_TOPIC = "metro-arrivals"


def create_consumer() -> Consumer:
    """Kafka Consumer 생성"""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    config = {
        "bootstrap.servers": bootstrap_servers,
        # TODO: Consumer Group ID 설정
        "group.id": "metro-consumer-group",
        # 가장 처음부터 읽기
        "auto.offset.reset": "earliest",
        # 수동 커밋 (처리 완료 후 커밋)
        "enable.auto.commit": False,
    }

    return Consumer(config)


def consume_and_save(consumer: Consumer, batch_size: int = 100,
                     output_dir: str = "/data"):
    """
    메시지를 소비하고 파일로 저장
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # TODO: 토픽 구독
    consumer.subscribe([KAFKA_TOPIC])

    print(f"Consumer 시작 - 토픽: {KAFKA_TOPIC}")
    print(f"배치 크기: {batch_size}")

    messages = []
    message_count = 0

    try:
        while True:
            # TODO: 메시지 폴링 (1초 타임아웃)
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # 타임아웃 - 새 메시지 없음
                if messages:
                    # 대기 중인 메시지가 있으면 저장
                    save_batch(messages, output_dir)
                    # TODO: 오프셋 커밋
                    consumer.commit()
                    messages = []
                continue

            if msg.error():
                raise KafkaException(msg.error())

            # 메시지 파싱 (잘못된 형식은 건너뛰기)
            try:
                value = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as e:
                print(f"⚠️ JSON 파싱 실패 (건너뛰기): {e}")
                continue

            messages.append(value)
            message_count += 1

            print(f"수신: {value.get('station_name')} - {value.get('arrival_message')}")

            # 배치 크기 도달 시 저장
            if len(messages) >= batch_size:
                save_batch(messages, output_dir)
                consumer.commit()
                print(f"배치 저장 완료: {len(messages)}개")
                messages = []

    except KeyboardInterrupt:
        print("\n종료 요청...")
    finally:
        # 남은 메시지 저장
        if messages:
            save_batch(messages, output_dir)
            consumer.commit()

        consumer.close()
        print(f"Consumer 종료 - 총 {message_count}개 메시지 처리")


def save_batch(messages: list, output_dir: str):
    """배치 단위로 파일 저장"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"kafka_batch_{timestamp}.json"
    filepath = Path(output_dir) / filename

    data = {
        "saved_at": datetime.now().isoformat(),
        "message_count": len(messages),
        "messages": messages
    }

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"저장: {filepath}")


if __name__ == "__main__":
    consumer = create_consumer()
    consume_and_save(consumer, batch_size=50)