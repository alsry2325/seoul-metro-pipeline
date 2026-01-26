"""
서울시 지하철 실시간 도착정보 API 클라이언트
"""
import os
import requests
from dotenv import load_dotenv

# .env 파일에서 환경변수 로드
load_dotenv()


class SeoulMetroAPI:
    """서울시 지하철 API 클라이언트"""

    BASE_URL = "http://swopenAPI.seoul.go.kr/api/subway"

    def __init__(self):
        # TODO: 환경변수에서 API 키 가져오기
        self.api_key = os.getenv("SEOUL_API_KEY")

        if not self.api_key:
            raise ValueError("SEOUL_API_KEY 환경변수가 설정되지 않았습니다.")

    def get_arrival_info(self, station_name: str) -> dict:
        """
        특정 역의 실시간 도착 정보를 조회합니다.

        Args:
            station_name: 역 이름 (예: "강남", "서울역")

        Returns:
            API 응답 딕셔너리
        """
        # TODO: API 엔드포인트 URL 완성하기
        # 형식: {BASE_URL}/{API_KEY}/json/realtimeStationArrival/0/10/{역이름}
        url = f"{self.BASE_URL}/{self.api_key}/json/realtimeStationArrival/0/10/{station_name}"

        try:
            # TODO: GET 요청 보내기
            response = requests.get(url)
            response.raise_for_status()

            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"API 호출 실패: {e}")
            return {}

    def get_multiple_stations(self, station_names: list) -> list:
        """
        여러 역의 도착 정보를 조회합니다.

        Args:
            station_names: 역 이름 리스트

        Returns:
            각 역의 응답을 담은 리스트
        """
        results = []

        for station in station_names:
            # TODO: 각 역의 도착 정보 조회하기
            data = self.get_arrival_info(station)

            if data:
                results.append({
                    "station": station,
                    "data": data
                })

        return results


# 테스트 코드
if __name__ == "__main__":
    api = SeoulMetroAPI()

    # 강남역 도착 정보 조회
    result = api.get_arrival_info("강남")
    print(f"조회 결과 키: {result.keys()}")

    if "realtimeArrivalList" in result:
        arrivals = result["realtimeArrivalList"]
        print(f"도착 정보 개수: {len(arrivals)}")

        # 첫 번째 도착 정보 출력
        if arrivals:
            first = arrivals[0]
            print(f"역명: {first.get('statnNm')}")
            print(f"방향: {first.get('trainLineNm')}")
            print(f"도착메시지: {first.get('arvlMsg2')}")