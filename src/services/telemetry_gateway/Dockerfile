FROM python:3.11-slim
WORKDIR /app
ENV PYTHONPATH=/app
RUN pip install fastapi uvicorn[standard] redis psycopg2-binary
COPY src/gds_shared ./gds_shared
COPY src/services/telemetry_gateway/main.py ./main.py
CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8080"]
