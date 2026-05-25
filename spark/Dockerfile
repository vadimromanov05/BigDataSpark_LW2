
FROM spark:3.5.0-python3

USER root

RUN pip install --no-cache-dir \
    clickhouse-driver==0.2.6   \
    cassandra-driver==3.29.1   \
    neo4j==5.18.0              \
    pymongo==4.6.1             \
    redis==5.0.1

USER spark
