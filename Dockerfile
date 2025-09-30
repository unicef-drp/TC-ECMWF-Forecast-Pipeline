FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    libeccodes0 \
    libeccodes-dev \
    libeccodes-tools \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ecmwf_tc_data_downloader.py .
COPY ecmwf_tc_data_extractor.py .
COPY ecmwf_tc_data_transformer.py .
COPY snowflake_loader.py .
COPY main.py .

RUN mkdir -p tc_data tc_data_transformed

CMD ["python", "main.py"]