#!/bin/bash
baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/../header.sh
echo $yesterday

listDataDest=${RESULT_LOCAL_DIR}list

if [ ! -d $listDataDest ];then
    mkdir -p $listDataDest
fi
beforeRunMRMoudle gentie

#跟帖URLTOP10数量统计
sh $baseDir/../runMRJob.sh -c com.netease.dadaOffline.statistics.WeeklySourceGenTieMR -d $yesterday

afterRunMRMoudle gentie 3 600 
if [ "$errorList" != "" ];then
	errorAlarm gentieinfo:$errorList
fi

${HADOOP} fs -text /ntes_weblog/dada/statistics/temp/genTieCount/$yesterday/p* |awk -F'\t' '{if($1=="all"){print $2"\t"$3}}'|sort -k2,2nr|head -n10 > $listDataDest/genTieTop_all_$yesterday
${HADOOP} fs -text /ntes_weblog/dada/statistics/temp/genTieCount/$yesterday/p* |awk -F'\t' '{if($1=="khd"){print $2"\t"$3}}'|sort -k2,2nr|head -n10 > $listDataDest/genTieTop_khd_$yesterday
${HADOOP} fs -text /ntes_weblog/dada/statistics/temp/genTieCount/$yesterday/p* |awk -F'\t' '{if($1=="pc"){print $2"\t"$3}}'|sort -k2,2nr|head -n10 > $listDataDest/genTieTop_pc_$yesterday
${HADOOP} fs -text /ntes_weblog/dada/statistics/temp/genTieCount/$yesterday/p* |awk -F'\t' '{if($1=="wap"){print $2"\t"$3}}'|sort -k2,2nr|head -n10 > $listDataDest/genTieTop_wap_$yesterday