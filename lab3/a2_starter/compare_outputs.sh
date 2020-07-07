#!/bin/bash

#OUTPUT_SPARK=output_spark
#OUTPUT_HADOOP=output_hadoop
#
#cat $OUTPUT_SPARK/* | sort > normalized_spark.txt
#cat $OUTPUT_HADOOP/* | sort > normalized_hadoop.txt

hdfs dfs -cat task1-hadoop.out.txt/* | sort > normalized_spark.txt
hdfs dfs -cat task1-spark.out.txt/* | sort > normalized_hadoop.txt

echo Diffing Spark and Hadoop task1 outputs:
diff normalized_spark.txt normalized_hadoop.txt

if [ $? -eq 0 ]; then
    echo Task1 Outputs match.
else
    echo Task1 Outputs do not match. Looks for bugs.
fi

rm normalized_spark.txt
rm normalized_hadoop.txt

hdfs dfs -cat task2-hadoop.out.txt/* | sort > normalized_spark.txt
hdfs dfs -cat task2-spark.out.txt/* | sort > normalized_hadoop.txt

echo Diffing Spark and Hadoop task2 outputs:
diff normalized_spark.txt normalized_hadoop.txt

if [ $? -eq 0 ]; then
    echo Task2 Outputs match.
else
    echo Task2 Outputs do not match. Looks for bugs.
fi

rm normalized_spark.txt
rm normalized_hadoop.txt


hdfs dfs -cat task3-hadoop.out.txt/* | sort > normalized_spark.txt
hdfs dfs -cat task3-spark.out.txt/* | sort > normalized_hadoop.txt

echo Diffing Spark and Hadoop task3 outputs:
diff normalized_spark.txt normalized_hadoop.txt

if [ $? -eq 0 ]; then
    echo Task3 Outputs match.
else
    echo Task3 Outputs do not match. Looks for bugs.
fi

rm normalized_spark.txt
rm normalized_hadoop.txt

hdfs dfs -cat task4-hadoop.out.txt/* | sort > normalized_spark.txt
hdfs dfs -cat task4-spark.out.txt/* | sort > normalized_hadoop.txt

echo Diffing Spark and Hadoop task4 outputs:
diff normalized_spark.txt normalized_hadoop.txt

if [ $? -eq 0 ]; then
    echo Task4 Outputs match.
else
    echo Task4 Outputs do not match. Looks for bugs.
fi

rm normalized_spark.txt
rm normalized_hadoop.txt




