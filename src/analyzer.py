"""
수집된 지하철 데이터 분석기
"""
import json
import pandas as pd
from pathlib import Path


# 호선 코드 매핑
SUBWAY_LINES = {
    "1001": "1호선",
    "1002": "2호선",
    "1003": "3호선",
    "1004": "4호선",
    "1005": "5호선",
    "1006": "6호선",
    "1007": "7호선",
    "1008": "8호선",
    "1009": "9호선",
    "1063": "경의중앙선",
    "1065": "공항철도",
    "1067": "경춘선",
    "1075": "수인분당선",
    "1077": "신분당선",
}


def load_json_to_dataframe(filepath: str) -> pd.DataFrame:
    """
    JSON 파일을 읽어 DataFrame으로 변환합니다.

    Args:
        filepath: JSON 파일 경로

    Returns:
        pandas DataFrame
    """
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    # TODO: arrivals 리스트를 DataFrame으로 변환
    df = pd.DataFrame(data["arrivals"])

    # 호선 이름 매핑 추가
    df["line_name"] = df["subway_id"].map(SUBWAY_LINES)

    return df


def analyze_by_station(df: pd.DataFrame) -> pd.DataFrame:
    """
    역별 도착 정보 개수를 집계합니다.

    Args:
        df: 도착 정보 DataFrame

    Returns:
        역별 집계 DataFrame
    """
    # TODO: station_name으로 그룹화하여 개수 세기
    station_counts = df.groupby("station_name").size()

    # 컬럼명 변경
    station_counts = station_counts.reset_index()
    station_counts.columns = ["station_name", "arrival_count"]

    # 내림차순 정렬
    station_counts = station_counts.sort_values("arrival_count", ascending=False)

    return station_counts


def analyze_by_line(df: pd.DataFrame) -> pd.DataFrame:
    """
    호선별 평균 대기 시간을 계산합니다.

    Args:
        df: 도착 정보 DataFrame

    Returns:
        호선별 집계 DataFrame
    """
    # 대기 시간을 숫자로 변환 (문자열인 경우)
    df["arrival_time_sec"] = pd.to_numeric(df["arrival_time_sec"], errors="coerce")

    # TODO: line_name으로 그룹화하여 평균 계산
    line_stats = df.groupby("line_name")["arrival_time_sec"].mean()

    line_stats = line_stats.reset_index()
    line_stats.columns = ["line_name", "avg_wait_sec"]

    # 평균 대기 시간 내림차순 정렬
    line_stats = line_stats.sort_values("avg_wait_sec", ascending=False)

    return line_stats


def save_analysis_results(station_df: pd.DataFrame, line_df: pd.DataFrame,
                         output_dir: str = "/output"):
    """
    분석 결과를 CSV 파일로 저장합니다.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    station_path = Path(output_dir) / "station_analysis.csv"
    line_path = Path(output_dir) / "line_analysis.csv"

    # TODO: DataFrame을 CSV로 저장 (인덱스 제외)
    station_df.to_csv(station_path, index=False, encoding="utf-8-sig")
    line_df.to_csv(line_path, index=False, encoding="utf-8-sig")

    print(f"역별 분석 저장: {station_path}")
    print(f"호선별 분석 저장: {line_path}")


if __name__ == "__main__":
    # 가장 최근 데이터 파일 찾기
    data_files = sorted(Path("/data").glob("arrivals_*.json"))

    if not data_files:
        print("분석할 데이터 파일이 없습니다. 먼저 collector.py를 실행하세요.")
        exit(1)

    latest_file = data_files[-1]
    print(f"분석 대상 파일: {latest_file}")

    # 데이터 로드
    df = load_json_to_dataframe(str(latest_file))
    print(f"\n전체 데이터 개수: {len(df)}")
    print(f"컬럼: {list(df.columns)}")

    # 역별 분석
    station_analysis = analyze_by_station(df)
    print(f"\n=== 역별 도착 정보 개수 ===")
    print(station_analysis.head(10))

    # 호선별 분석
    line_analysis = analyze_by_line(df)
    print(f"\n=== 호선별 평균 대기 시간 ===")
    print(line_analysis)

    # 결과 저장
    save_analysis_results(station_analysis, line_analysis)