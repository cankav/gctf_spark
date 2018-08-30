MAIN_PY=$1

echo "running $MAIN_PY"

if [ $MAIN_PY = "gtp.py" ]
then
    rm /tmp/_gtp*.csv
    /home/sprk/hadoop-2.9.1/bin/hadoop fs -rm -r /gctf_data/gtp_test_output.csv /gctf_data/gtp_test_input1.csv /gctf_data/gtp_test_input2.csv
    $HADOOP_HOME/bin/hadoop fs -rm -r hdfs://spark-master0-dsl05:9000/gctf_data/_gtp*.csv
elif [ $MAIN_PY == 'gctf.py' ]
then
    rm /tmp/_gtp*.csv /tmp/gctf_test_*.csv
    $HADOOP_HOME/bin/hadoop fs -rm -r hdfs://spark-master0-dsl05:9000/gctf_data/_gtp*.csv hdfs://spark-master0-dsl05:9000/gctf_data/gctf_test_*.csv
fi

export HADOOP_USER_NAME=sprk;
# --total-executor-cores 2
/home/sprk/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --master spark://spark-master0-dsl05:7077 --conf "spark.executor.memory=5500m"  --py-files utils.py,gtp.py,hadamard.py $MAIN_PY
