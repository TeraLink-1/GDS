FROM python:3.11-slim
WORKDIR /app
ENV PYTHONPATH=/app
RUN pip install redis
COPY src/gds_shared ./gds_shared
COPY src/services/gs_control/main.py ./main.py
CMD ["python", "main.py"]
