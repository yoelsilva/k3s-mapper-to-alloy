FROM python:3.12-slim

LABEL org.opencontainers.image.title="dependencias-mapper" \
      org.opencontainers.image.description="Mapa de dependencias declaradas de un clúster Kubernetes, expuesto como métricas Prometheus" \
      org.opencontainers.image.source="https://github.com/threeface/dependencias-mapper"

# Sin dependencias externas: solo librería estándar.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CONFIG=/config/config.json \
    PORT=9400

RUN useradd --system --uid 65534 --no-create-home mapper 2>/dev/null || true

WORKDIR /app
COPY mapper.py /app/mapper.py

USER 65534
EXPOSE 9400

ENTRYPOINT ["python3", "/app/mapper.py"]
