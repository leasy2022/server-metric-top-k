#!/bin/bash

source $1

if [ ${local_debug} != 1 ]; then
	ymdh=`date +%Y%m%d%H -d '-1 hours'`
	ymd=${ymdh:0:8}
	hour=${ymdh:8}
	timestamp=`date -d "${ymd} ${hour}" +%s`

	last_ymdh=`date +%Y%m%d%H -d '-2 hours'`
	last_ymd=${last_ymdh:0:8}
	last_hour=${last_ymdh:8}

	source_dir=${source_root_dir}/${ymdh}
	es_dir=${es_root_dir}/${ymd}/${hour}
	last_result_dir=${result_root_dir}/${last_ymd}/${last_hour}
	cur_result_dir=${result_root_dir}/${ymd}/${hour}
else
	source_dir=/Users/wushang/Downloads/spark/toytest/action-external-metrics/rawdata/2017041110
	last_result_dir=/Users/wushang/Downloads/spark/toytest/action-external-metrics/aggr/20170411/09
	es_dir=/Users/wushang/Downloads/spark/toytest/action-external-metrics/es/20170411/10
	cur_result_dir=/Users/wushang/Downloads/spark/toytest/action-external-metrics/aggr/20170411/10
	timestamp=1491876000
fi

echo "[source_dir="${source_dir}"]"
echo "[last_result_dir="${last_result_dir}"]"
echo "[es_dir="${es_dir}"]"
echo "[cur_result_dir="${cur_result_dir}"]"
echo "[url=$mysql_jdbc_url]"
echo "[timestamp=$timestamp]"

spark-submit \
  --class "newlens.metric.topk.TopProcessor" \
  --master ${spark_master} \
  --name metric-topk-${ymdh} \
  ${application_jar} \
  ${source_dir} ${last_result_dir} ${es_dir} ${cur_result_dir} ${timestamp} ${mysql_jdbc_url} ${mysql_user} ${mysql_password} ${mysql_table_name} ${topK}


# --deploy-mode standalone \
#  --jars mysql \

