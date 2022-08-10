sudo mkdir -p /opt/hive-conf/
sudo chmod 777 -R /opt/hive-conf/
nano /opt/hive-conf/hive-site.xml

copy the content from https://github.com/nodesense/fastdata-stack/blob/main/flink/hive-conf/hive-site.xml
 
nano ~/.m2/settings.xml

paste the content from https://github.com/nodesense/flink-workshop/blob/main/m2/settings.xml

Ctrl + O  =Enter to save

Ctrl +X to exit





<dependency>
<groupId>org.apache.flink</groupId>
<artifactId>flink-table-planner_2.12</artifactId>
<version>1.15.1</version>
<scope>provided</scope>
</dependency>



Download 

https://files.grouplens.org/datasets/movielens/ml-latest-small.zip


https://github.com/nodesense/cts-data-engineering-feb-2022/blob/main/notes/006-hive-database.md

http://localhost:9870/dfshealth.html#tab-overview

copy the extracted csv files without header into  data directory [create one]


~/fastdata-stack/flink/mount/data

now in portainer, open console in the hadoop-client container.

now in portainer, open console in the hadoop-client container.

copy the /data/ml-latest-small to hadoop

hdfs dfs -put /data/ml-latest-small hdfs://namenode:9000/

check files are available..

hdfs dfs -ls hdfs://namenode:9000/ml-latest-small

in browser, http://localhost:9870