#!/bin/bash

source $1

if [ ${local_debug} != 1 ]; then
	ymdh=`date +%Y%m%d%H -d '-1 hours'`
	last_ymdh=`date +%Y%m%d%H -d '-2 hours'`
else
	ts1=$(($debug_execute_timestamp - 60*60))
    ts2=$(($debug_execute_timestamp - 60*60*2))
    ymdh=`date -d @${ts1} +"%Y%m%d%H"`
    last_ymdh=`date -d @${ts2} +"%Y%m%d%H"`
fi

ymd=${ymdh:0:8}
hour=${ymdh:8}
timestamp=`date -d "${ymd} ${hour}" +%s`
last_ymd=${last_ymdh:0:8}
last_hour=${last_ymdh:8}

source_dir=${source_root_dir}/${ymdh}
es_dir=${es_root_dir}/${ymd}/${hour}
last_result_dir=${result_root_dir}/${last_ymd}/${last_hour}
cur_result_dir=${result_root_dir}/${ymd}/${hour}

echo "[source_dir="${source_dir}"]"
echo "[last_result_dir="${last_result_dir}"]"
echo "[es_dir="${es_dir}"]"
echo "[cur_result_dir="${cur_result_dir}"]"
echo "[url=$mysql_jdbc_url]"
echo "[timestamp=$timestamp]"

# 还需要添加 依赖包 和 参数配置
spark-submit \
  --class "newlens.metric.topk.TopProcessor" \
  --master ${spark_master} \
  --name metric-topk-${ymdh} \
  ${application_jar} \
  ${source_dir} ${last_result_dir} ${es_dir} ${cur_result_dir} ${timestamp} ${mysql_jdbc_url} ${mysql_user} ${mysql_password} ${mysql_table_name} ${insert_batch} ${topK} ${max_metric_length}


# --deploy-mode standalone \

