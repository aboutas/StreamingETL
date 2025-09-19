FROM flink:1.18.1-scala_2.12-java17

COPY target/etl-flink-1.0.0.jar /opt/flink/usrlib/

ENV FLINK_PROPERTIES="jobmanager.rpc.address: flink-jobmanager"