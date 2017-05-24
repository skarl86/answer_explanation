#! /bin/bash
if [[ $# != 3 ]];
then
    echo "Usage: query_path triple_hdfs_path result_hdfs_path_for_save"
    exit 1
fi
echo -e "\033[32m Start Building Answer Explanation\033[0m"
sbt package
echo -e "\033[32m End Building Answer Explanation\033[0m"
query_result="query_result"
hadoop fs -rm -r $query_result $3

echo -e "\033[32m Start Executing Query '$1'\033[0m"
bash ../sparqlgx/bin/sparqlgx.sh query -o $query_result nk-test $1
echo -e "\033[32m End Executing Query '$1'\033[0m"

echo -e "\033[32m Start Answer Explanation\033[0m"
spark-submit --class UnifyResult \
        --master spark://192.168.100.72:7077 \
        --executor-memory 30G \
        --total-executor-cores 620 \
        --conf spark.eventLog.enabled=false \
        target/scala-2.10/answer_explanation_2.10-1.0.jar $query_result $1 $2 $3
echo -e "\033[32m End Answer Explanation\033[0m"
