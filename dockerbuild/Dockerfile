FROM registry.b2w.io/atlas/openjdk8:1.8.0_40-internal

ADD dmon-api-assembly-1.0.jar /opt/dmon-api.jar

ADD spark-2.tar /opt

ADD addHosts.sh /opt/addHosts.sh

RUN chmod +x /opt/addHosts.sh

CMD ["ulimit -n 64000; /opt/spark-2.0.2-bin-hadoop2.7/bin/./spark-submit --master local[*] --driver-memory 4g --executor-memory 6g --conf spark.shuffle.sort.bypassMergeThreshold=50 --conf spark.driver.extraJavaOptions=\"$JAVA_OPTS\" --conf spark.sql.hive.convertMetastoreParquet=false /opt/dmon-api.jar"]

ENTRYPOINT ["/opt/addHosts.sh"]
