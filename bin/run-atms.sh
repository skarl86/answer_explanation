#! /bin/bash
hadoop fs -rm -R output

spark-submit --class AtmsBuilder \
	--master spark://192.168.100.72:7077 \
	--executor-memory 15G \
	--total-executor-cores 620 \
	--conf spark.eventLog.enabled=false \
	lib/atms_proj1_2.10-1.0.jar


