FROM apache/superset:4.0.2

USER root

RUN pip install --no-cache-dir clickhouse-connect==0.7.19

COPY scripts /app/scripts
RUN chmod +x /app/scripts/*.sh

USER superset
