"""
지하철 도착 정보 수집기
"""
import json
from datetime import datetime
from pathlib import Path
from src.api_client import SeoulMetroAPI


# 수집할 주요 역 목록
TARGET_STATIONS = [
    "강남", "홍대입구", "서울역"
]


def collect_arrivals(stations: list = None) -> dict:
    """
    여러 역의 도착 정보를 수집합니다.

    Args:
        stations: 수집할 역 목록 (None이면 TARGET_STATIONS 사용)

    Returns:
        수집된 데이터 딕셔너리
    """
    if stations is None:
        stations = TARGET_STATIONS

    api = SeoulMetroAPI()

    # TODO: 현재 시간 기록 (ISO 형식)
    collected_at = datetime.now().isoformat()

    all_arrivals = []

    for station in stations:
        print(f"수집 중: {station}...")
        result = api.get_arrival_info(station)

        if "realtimeArrivalList" in result:
            for arrival in result["realtimeArrivalList"]:
                # 필요한 필드만 추출
                # TODO: 딕셔너리에서 필요한 값 추출하기
                processed = {
                    "station_name": arrival.get("statnNm"),
                    "subway_id": arrival.get("subwayId"),
                    "train_line": arrival.get("trainLineNm"),
                    "arrival_message": arrival.get("arvlMsg2"),
                    "arrival_time_sec": arrival.get("barvlDt"),
                    "received_at": arrival.get("recptnDt"),
                    "collected_at": collected_at
                }
                all_arrivals.append(processed)

    return {
        "collected_at": collected_at,
        "station_count": len(stations),
        "arrival_count": len(all_arrivals),
        "arrivals": all_arrivals
    }


def save_to_json(data: dict, output_dir: str = None) -> str:
    # Docker: /data (볼륨 마운트), GitHub Actions/로컬: ./data
    import os
    if output_dir is None:
        # 환경변수 우선, 없으면 /data 존재 여부로 판단
        output_dir = os.environ.get("DATA_DIR")
        if output_dir is None:
            output_dir = "/data" if os.path.isdir("/data") else "./data"

    # raw 서브디렉토리에 저장 (GitHub Actions 워크플로우와 일치)
    raw_dir = Path(output_dir) / "raw"
    
    raw_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"arrivals_{timestamp}.json"
    filepath = raw_dir / filename

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"저장 완료: {filepath}")
    return str(filepath)


if __name__ == "__main__":
    # 데이터 수집
    data = collect_arrivals()

    print(f"\n수집 결과:")
    print(f"- 수집 시간: {data['collected_at']}")
    print(f"- 역 개수: {data['station_count']}")
    print(f"- 도착 정보 개수: {data['arrival_count']}")

    # 파일 저장
    filepath = save_to_json(data)
    print(f"- 저장 파일: {filepath}")