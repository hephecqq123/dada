#!/bin/bash
baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/../header.sh
echo $yesterday

listDataDest=${RESULT_LOCAL_DIR}list

if [ ! -d $listDataDest ];then
    mkdir -p $listDataDest
fi

beforeRunMRMoudle shareBackInfo

#每周的分享数和分享稿件top10
sh $baseDir/../runMRJob.sh -c com.netease.dadaOffline.statistics.WeeklyArticleShareTop10MR -d $yesterday
#每周的回流数和回流稿件top10
sh $baseDir/../runMRJob.sh -c com.netease.dadaOffline.statistics.WeeklyArticleBackTop10MR -d $yesterday
#每周的分享数和分享稿件top10 keyFromUuidToUrl
sh $baseDir/../runMRJob.sh -c com.netease.dadaOffline.statistics.UuidOfBackTop10ToUrlMR -d $yesterday
sh $baseDir/../runMRJob.sh -c com.netease.dadaOffline.statistics.UuidOfShareTop10ToUrlMR -d $yesterday

afterRunMRMoudle shareBackInfo 3 600 
if [ "$errorList" != "" ];then
	errorAlarm pvuv:$errorList
fi

${HADOOP} fs -text /ntes_weblog/dada/statistics/temp/weeklyArticleShareTop10Url/${yesterday}/p*  |sort -k 2,2nr |head -n 10 > $listDataDest/dadaShareTop_$yesterday
${HADOOP} fs -text /ntes_weblog/dada/statistics/temp/weeklyArticleBackTop10Url/${yesterday}/p*  |sort -k 2,2nr |head -n 10 > $listDataDest/dadaBackTop_$yesterday