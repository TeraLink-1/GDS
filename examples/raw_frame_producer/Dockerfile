FROM python:3.11-slim
WORKDIR /app
ENV PYTHONPATH=/app
RUN pip install redis
COPY src/gds_shared ./gds_shared
COPY examples/raw_frame_producer/producer.py ./producer.py
CMD ["python","producer.py"]
