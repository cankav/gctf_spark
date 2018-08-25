MAIN_PY=$1

echo "running $MAIN_PY"

if [ $MAIN_PY = "gtp.py" ]
then
    /home/sprk/hadoop-2.9.1/bin/hadoop fs -rm -r /gctf_data/gtp_test_output.csv
elif [ $MAIN_PY == 'gctf.py' ]
then
    rm /tmp/_gtp*.csv
    #$HADOOP_HOME/bin/hadoop fs -rm -r hdfs://spark-master0-dsl05:9000/gctf_data/_gtp*.csv
fi

export HADOOP_USER_NAME=sprk;
/home/sprk/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --master spark://spark-master0-dsl05:7077 --conf "spark.executor.memory=5500m" --total-executor-cores 2 --py-files utils.py,gtp.py,hadamard.py,generate_random_tensor_data.py $MAIN_PY
