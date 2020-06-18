
# TaxpaymentAnalysis 

## Following Steps provide details on how to run dataapp for cleaning, joining and mapping data using Spark/HDFS integration.

## Pre-requisites
1. Setup Java 8 and Apache Maven(https://maven.apache.org/install.html) to build code.
2. Setup HDFS/Hadoop(Version 2.6.5) to run in Pseudo distributed mode(Follow Instruction in provide URL): https://hadoop.apache.org/docs/r2.6.5/hadoop-project-dist/hadoop-common/SingleCluster.html
3. Setup Spark Cluster(2.4.5): https://spark.apache.org/docs/latest/spark-standalone.html
Make sure to update your conf/spark-env.sh with following configs
```
export JAVA_HOME=<PATH_TO_JAVA_SDK>
export HADOOP_CONF_DIR=<PATH_TO_HADOOP_CONF_DIR>

```
Typically hadoop conf directory is the directory where you have unzipped the hadoop directory followed by etc/hadoop.

## Building the datapp code.
```
> mvn clean package
```
### Copy the build Jar
```
>  cp target/SparkDataJava-1.0-SNAPSHOT-jar-with-dependencies.jar  <spark_installation_directory>
```

### Running the Spark Application built using dataapp
```shell script
bin/spark-submit --class SparkApp --master <spark_master_url>  SparkDataJava-1.0-SNAPSHOT-jar-with-dependencies.jar  -i <hdfs path to directory to parse and process> -o <HDFS Path to Directory> -c <HDFS PATH to uszips.csv
```
#### Sample example
```shell script
bin/spark-submit --class SparkApp --master spark://localhost:7077 --conf spark.eventLog.enabled=true SparkDataJava-1.0-SNAPSHOT-jar-with-dependencies.jar  -i hdfs://localhost:9000/user/mrunaldeshmukh/datafiles/ -o hdfs://localhost:9000/user/mrunaldeshmukh/spark-output -c hdfs://localhost:9000/user/mrunaldeshmukh/zips/uszips.csv
```

  
