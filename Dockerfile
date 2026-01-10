FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY auto_updater.py .
COPY lighter_data.db .

CMD ["python", "auto_updater.py"]
