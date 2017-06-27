#! /bin/bash
triple="$1"
rule="$2"

hadoop fs -rm -R atmsOutput
hadoop fs -rm -R nk
spark-submit --class SWRL_Reasoner \
	--master spark://192.168.100.72:7077 \
	--driver-memory 60G \
	--executor-memory 60G \
	--total-executor-cores 620 \
	--conf spark.eventLog.enabled=false \
	--conf spark.driver.maxResultSize=20g \
	lib/swrl_reasoner_v2_2.10-1.0.jar $triple $rule
