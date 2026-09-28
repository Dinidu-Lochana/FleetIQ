FROM python:3.11-slim

WORKDIR /app

COPY api/requirements.txt /app/api-requirements.txt
RUN pip install --no-cache-dir -r /app/api-requirements.txt

COPY common /app/common
COPY api/main.py api/db.py /app/
COPY api/static /app/static

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
