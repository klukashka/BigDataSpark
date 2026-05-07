FROM bitnami/spark:3.5

USER root

# JDBC drivers
RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -fsSL -o /opt/bitnami/spark/jars/postgresql.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar && \
    curl -fsSL -o /opt/bitnami/spark/jars/clickhouse-jdbc.jar https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.3/clickhouse-jdbc-0.6.3.jar

USER 1001
