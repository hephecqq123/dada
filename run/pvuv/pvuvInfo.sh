#!/bin/bash
baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/../header.sh
echo $yesterday


beforeRunMRMoudle pvuv

#跟帖URLTOP10数量统计
sh $baseDir/../runMRJob.sh -c com.netease.dadaOffline.statistics.DailyColumnPvUvMR -d $yesterday

afterRunMRMoudle pvuv 3 600 
if [ "$errorList" != "" ];then
	errorAlarm pvuv:$errorList
fi

