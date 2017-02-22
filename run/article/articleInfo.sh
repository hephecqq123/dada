#!/bin/bash
baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/../header.sh
echo $yesterday


beforeRunMRMoudle article

#跟帖URLTOP10数量统计
sh $baseDir/../runMRJob.sh -c com.netease.dadaOffline.statistics.DailyArticleIncrCountMR -d $yesterday

afterRunMRMoudle article 3 600
if [ "$errorList" != "" ];then
	errorAlarm article:$errorList
fi

