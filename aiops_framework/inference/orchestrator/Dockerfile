FROM python:3.11-slim

WORKDIR /app

ENV PYTHONPATH=/app

COPY aiops_framework/inference/orchestrator/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY aiops_framework /app/aiops_framework

EXPOSE 8002

CMD ["python", "-m", "uvicorn", "aiops_framework.inference.orchestrator.app:app", "--host", "0.0.0.0", "--port", "8002"]
