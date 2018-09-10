#!/bin/bash

source $1

ymdh=`date +%Y%m%d%H -d '-1 hours'`
ym=${ymdh:0:6}
ymd=${ymdh:0:8}
hour=${ymdh:8}

source_dir=${source_dir}/${ymd}/${hour}
#es_index=${es_index}-${ymdh}
es_index=${es_index}-${ym}

echo "[application_jar=${application_jar}]"
echo "[spark_master=${spark_master}]"
echo "[app_name=${app_name}]"
echo "[source_dir=${source_dir}]"
echo "[es_hosts=${es_hosts}]"
echo "[es_port=${es_port}]"
echo "[es_index=${es_index}]"
echo "[es_type=${es_type}]"

spark-submit \
  --class "newlens.metric.top.EsSink" \
  --master ${spark_master} \
  ${application_jar} \
  ${spark_master} ${app_name}  ${source_dir} ${es_hosts} ${es_port} ${es_index} ${es_type}


