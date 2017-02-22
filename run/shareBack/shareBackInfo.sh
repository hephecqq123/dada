#!/bin/bash
baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/../header.sh
echo $yesterday


beforeRunMRMoudle shareBackInfo

#所有哒哒文章的每日分享回流次数
sh $baseDir/../runMRJob.sh -c com.netease.dadaOffline.statistics.DailyArticleShareBackCountMR -d $yesterday
sh $baseDir/../runMRJob.sh -c com.netease.dadaOffline.statistics.DailyArticleShareBackUuidCountMR -d $yesterday


afterRunMRMoudle shareBackInfo 3 600 
if [ "$errorList" != "" ];then
	errorAlarm pvuv:$errorList
fi

