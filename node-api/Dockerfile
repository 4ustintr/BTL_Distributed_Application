# Build context là gốc repo (cần common/):
#   docker build -f node-api/Dockerfile -t node-api .
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY node-api/ ./

# Trong container luôn nghe 8000; compose ánh xạ 8001–8003 -> 8000 (F2)
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
