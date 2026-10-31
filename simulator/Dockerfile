FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY tidewater_sim ./tidewater_sim

ENTRYPOINT ["python", "-m", "tidewater_sim"]
