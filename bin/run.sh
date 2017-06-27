#! /bin/bash
if [[ $# != 2 ]];
then
    echo "Usage: sparqlgx_db_name query_path"
    exit 1
fi
echo -e "\033[32mStart Building Answer Explanation\033[0m"
sbt package
echo -e "\033[32mEnd Building Answer Explanation\033[0m"

query_result="query_result"
tmp_query_result="tmp_query_result"
db_name="$1"
query_path="$2"

IFS='/' read -r -a array <<< "$query_path"
tmp_query_path="query/tmp_lubm/${array[-1]}"

hadoop fs -rm -r $query_result $tmp_query_result

echo -e "\033[32mStart Executing Query '${array[-1]}'\033[0m"
bash ../sparqlgx/bin/sparqlgx.sh query -o $query_result $db_name $query_path
echo -e "\033[32mEnd Executing Query '${array[-1]}'\033[0m"

echo -e "\033[32mStart Preprocessing for Explanation '${array[-1]}'\033[0m"
bash ../sparqlgx/bin/sparqlgx.sh query -o $tmp_query_result $db_name $tmp_query_path
echo -e "\033[32mEnd Preprocessing for Explanation '${array[-1]}'\033[0m"

echo -e "\033[32mStart Answer Explanation\033[0m"
spark-submit --class UnifyResult \
	--master spark://192.168.100.72:7077 \
	--executor-memory 30G \
	--total-executor-cores 620 \
	--conf spark.eventLog.enabled=false \
	target/scala-2.10/answer_explanation_2.10-1.0.jar $query_result $tmp_query_result $tmp_query_path $query_path
echo -e "\033[32mEnd Answer Explanation\033[0m"
