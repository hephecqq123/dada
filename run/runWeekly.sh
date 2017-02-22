#!/bin/bash
baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/header.sh

listDataDest=${RESULT_LOCAL_DIR}list

if [ ! -d $listDataDest ];then
    mkdir -p $listDataDest
fi

function main() {
	##--统计------------------------
	sh $baseDir/gentie/gentieInfo.sh $yesterday
	sh $baseDir/shareBack/weeklyShareBackInfo.sh $yesterday
	sh $baseDir/weeklypvuv/weeklyPvUvInfo.sh $yesterday
	
	##--导入dc------------------------
	#toDcList
	sh $baseDir/toDc.sh $yesterday
}
main
