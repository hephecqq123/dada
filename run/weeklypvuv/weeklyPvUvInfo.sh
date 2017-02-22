#!/bin/bash
baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/../header.sh
echo $yesterday
listDataDest=${RESULT_LOCAL_DIR}list

if [ ! -d $listDataDest ];then
    mkdir -p $listDataDest
fi

beforeRunMRMoudle weeklypvuv

#跟帖URLTOP10数量统计
sh $baseDir/../runMRJob.sh -c com.netease.dadaOffline.statistics.WeeklyPvUvTop10MR -d $yesterday
sh $baseDir/../runMRJob.sh -c com.netease.dadaOffline.statistics.UuidOfPvTop10ToUrlMR -d $yesterday
sh $baseDir/../runMRJob.sh -c com.netease.dadaOffline.statistics.UuidOfUvTop10ToUrlMR -d $yesterday

afterRunMRMoudle weeklypvuv 3 600 
if [ "$errorList" != "" ];then
	errorAlarm pvuv:$errorList
fi


${HADOOP} fs -text /ntes_weblog/dada/statistics/temp/weeklyArticlePvTop10Url/${yesterday}/p*  |sort -k 2,2nr |head -n 10 > $listDataDest/dadaPvTop_$yesterday
${HADOOP} fs -text /ntes_weblog/dada/statistics/temp/weeklyArticleUvTop10Url/${yesterday}/p*  |sort -k 2,2nr |head -n 10 > $listDataDest/dadaUvTop_$yesterday