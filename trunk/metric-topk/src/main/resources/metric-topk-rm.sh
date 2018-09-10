#!/bin/bash

root_dir="/action-external-metrics"

ymdh=`date +%Y%m%d%H -d '-24 hours'`
ymd=${ymdh:0:8}
hour=${ymdh:8}



raw_data_dir=${root_dir}/rawdata/${ymdh}
es_data_dir=${root_dir}/es/${ymd}/${hour}
result_data_dir=${root_dir}/result/${ymd}/${hour}

echo "raw_data_dir=${raw_data_dir}"
echo "es_data_dir=${es_data_dir}"
echo "result_data_dir=${result_data_dir}"

hadoop fs -rm -r  ${raw_data_dir}
hadoop fs -rm -r  ${raw_data_dir}
hadoop fs -rm -r  ${raw_data_dir}


