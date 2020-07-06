#!/bin/bash

#OUTPUT_SPARK=output_spark
#OUTPUT_HADOOP=output_hadoop
#
#cat $OUTPUT_SPARK/* | sort > normalized_spark.txt
#cat $OUTPUT_HADOOP/* | sort > normalized_hadoop.txt

hdfs dfs -cat a2_t1_output_hadoop/* | sort > normalized_spark.txt
hdfs dfs -cat a2_t1_output_spark/* | sort > normalized_hadoop.txt

echo Diffing Spark and Hadoop outputs:
diff normalized_spark.txt normalized_hadoop.txt

if [ $? -eq 0 ]; then
    echo Outputs match.
else
    echo Outputs do not match. Looks for bugs.
fi

rm normalized_spark.txt
rm normalized_hadoop.txt




