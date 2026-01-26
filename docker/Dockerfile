# docker/Dockerfile.python

# TODO: Python 3.11 slim 이미지를 베이스로 사용
FROM python:3.11-slim

# 작업 디렉토리 설정
WORKDIR /app

# TODO: requirements.txt를 컨테이너로 복사
COPY requirements.txt .

# TODO: pip로 의존성 설치 (캐시 사용 안 함)
RUN pip install --no-cache-dir -r requirements.txt

# 소스 코드 복사
COPY src/ ./src

# 환경 변수 설정 (Python 출력 버퍼링 비활성화)
ENV PYTHONUNBUFFERED=1

# 기본 명령어
CMD ["python", "-m", "src.collector"]