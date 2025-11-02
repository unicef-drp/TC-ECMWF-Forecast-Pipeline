FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    libeccodes0 \
    libeccodes-dev \
    libeccodes-tools \
    gcc \
    g++ \
    libgrib-api-dev \
    libproj-dev \
    libgeos-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy core pipeline modules from root
COPY ecmwf_tc_data_downloader.py .
COPY ecmwf_tc_data_extractor.py .
COPY ecmwf_tc_data_transformer.py .
COPY ecmwf_wind_data_downloader.py .
COPY ecmwf_wind_data_extractor.py .
COPY ecmwf_tc_wind_combination.py .

# Copy GitHub Actions specific files
COPY github_actions/snowflake_loader.py .
COPY github_actions/main.py .

RUN mkdir -p tc_data tc_data_transformed wind_data wind_extracted

CMD ["python", "main.py"]