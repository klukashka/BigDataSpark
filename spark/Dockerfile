FROM apache/spark:4.0.2-python3

USER root

# Ensure Spark CLIs are available without full path
ENV PATH="/opt/spark/bin:${PATH}"

# JDBC drivers
RUN mkdir -p /opt/spark/jars && \
    curl -fsSL -o /opt/spark/jars/postgresql.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar && \
    curl -fsSL -o /opt/spark/jars/clickhouse-jdbc-all.jar https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.5/clickhouse-jdbc-0.6.5-all.jar && \
    curl -fsSL -o /opt/spark/jars/clickhouse-client.jar https://repo1.maven.org/maven2/com/clickhouse/clickhouse-client/0.6.5/clickhouse-client-0.6.5.jar && \
    curl -fsSL -o /opt/spark/jars/clickhouse-http-client-shaded.jar https://repo1.maven.org/maven2/com/clickhouse/clickhouse-http-client/0.6.5/clickhouse-http-client-0.6.5-shaded.jar

USER root
