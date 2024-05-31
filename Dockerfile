FROM flink:1.18.1

RUN mkdir /opt/flink/usrlib
ADD target/flink-sql-runner-*.jar /opt/flink/usrlib/sql-runner.jar
