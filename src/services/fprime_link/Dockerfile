FROM python:3.11-slim
WORKDIR /app
ENV PYTHONPATH=/app
RUN pip install fastapi uvicorn[standard] redis
COPY src/gds_shared ./gds_shared
COPY src/services/fprime_link/main.py ./main.py
EXPOSE 50000 50001
CMD ["python", "main.py"]
