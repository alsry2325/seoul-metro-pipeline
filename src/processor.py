"""
데이터 처리기 - 수집된 JSON을 분석/가공
"""
import json
import os
from pathlib import Path
from datetime import datetime


def load_raw_data(data_dir: str = None) -> list:
    """raw 디렉토리의 JSON 파일들을 로드"""
    if data_dir is None:
        data_dir = os.environ.get("DATA_DIR")
        if data_dir is None:
            data_dir = "/data" if os.path.isdir("/data") else "./data"

    raw_dir = Path(data_dir) / "raw"
    all_arrivals = []

    if not raw_dir.exists():
        print(f"raw 디렉토리 없음: {raw_dir}")
        return all_arrivals

    for json_file in raw_dir.glob("*.json"):
        print(f"로드 중: {json_file}")
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            arrivals = data.get("arrivals", [])
            all_arrivals.extend(arrivals)

    return all_arrivals


def process_data(arrivals: list) -> dict:
    """도착 정보 집계"""
    if not arrivals:
        return {"station_stats": [], "line_stats": [], "total_count": 0}

    # 역별 집계
    station_counts = {}
    for arr in arrivals:
        station = arr.get("station_name", "Unknown")
        station_counts[station] = station_counts.get(station, 0) + 1

    station_stats = [
        {"station_name": k, "count": v}
        for k, v in sorted(station_counts.items(), key=lambda x: -x[1])
    ]

    # 호선별 집계
    line_mapping = {
        "1001": "1호선", "1002": "2호선", "1003": "3호선",
        "1004": "4호선", "1005": "5호선", "1006": "6호선",
        "1007": "7호선", "1008": "8호선", "1009": "9호선",
    }
    line_counts = {}
    for arr in arrivals:
        subway_id = arr.get("subway_id", "")
        line_name = line_mapping.get(subway_id, "기타")
        line_counts[line_name] = line_counts.get(line_name, 0) + 1

    line_stats = [
        {"line_name": k, "count": v}
        for k, v in sorted(line_counts.items(), key=lambda x: -x[1])
    ]

    return {
        "station_stats": station_stats,
        "line_stats": line_stats,
        "total_count": len(arrivals)
    }


def save_processed(result: dict, data_dir: str = None) -> str:
    """처리 결과 저장"""
    if data_dir is None:
        data_dir = os.environ.get("DATA_DIR")
        if data_dir is None:
            data_dir = "/data" if os.path.isdir("/data") else "./data"

    processed_dir = Path(data_dir) / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = processed_dir / f"processed_{timestamp}.json"

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"처리 결과 저장: {filepath}")
    return str(filepath)


if __name__ == "__main__":
    print("=" * 50)
    print("데이터 처리 시작")
    print("=" * 50)

    # 1. 데이터 로드
    arrivals = load_raw_data()
    print(f"\n로드된 도착 정보: {len(arrivals)}건")

    if not arrivals:
        print("처리할 데이터가 없습니다.")
        exit(0)

    # 2. 데이터 처리
    result = process_data(arrivals)

    print(f"\n처리 결과:")
    print(f"- 전체 레코드: {result['total_count']}건")
    print(f"- 역 수: {len(result['station_stats'])}개")
    print(f"- 호선 수: {len(result['line_stats'])}개")

    # 3. 결과 저장
    filepath = save_processed(result)

    print("\n데이터 처리 완료!")