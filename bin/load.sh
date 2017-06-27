#! /bin/bash
if [[ $# != 4 ]];
then
 echo "Usage: sparqlgx_dbname rule_path_in_local input_triple_path_in_hdfs inferred_triple_path_in_hdfs"
 exit 1
fi

sparqlgx_db="$1"
input_triple="$3"
inferred_triple="$4"
rule="$2"

# 1 Reasoning
echo -e "\033[32mStart Reasoning\033[0m"
bash run-swrl.sh $input_triple $rule
echo -e "\033[32mEnd Reasoning\033[0m"

# 2 Build ATMS
echo -e "\033[32mStart Building ATMS\033[0m"
bash run-atms.sh
echo -e "\033[32mEnd Building ATMS\033[0m"

# 3 Load SPARQLGX
echo -e "\033[32mStart Loading SPARQLGX\033[0m"
bash sparqlgx/bin/sparqlgx.sh remove $sparqlgx_db
bash sparqlgx/bin/sparqlgx.sh load $sparqlgx_db $input_triple,$inferred_triple
echo -e "\033[32mEnd Loading SPARQLGX\033[0m"
