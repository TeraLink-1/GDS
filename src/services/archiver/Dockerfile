FROM python:3.11-slim
WORKDIR /app
ENV PYTHONPATH=/app
RUN pip install redis psycopg2-binary
COPY src/gds_shared ./gds_shared
COPY src/services/archiver/main.py ./main.py
CMD ["python", "main.py"]
