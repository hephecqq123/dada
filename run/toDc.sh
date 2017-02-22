#!/bin/bash

baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/header.sh

listDataDest=${RESULT_LOCAL_DIR}list

if [ ! -d $listDataDest ];then
    mkdir -p $listDataDest
fi

echo $yesterday
function main {
	#toDc
	${HADOOP} jar ${JAR} com.netease.dadaOffline.common.DataExporter /ntes_weblog/dada/statistics/result_toDC/ $yesterday http://datacube.ws.netease.com/expdata/importData.do
	
	#toDcList
	/usr/bin/rsync -au $listDataDest/ 10.130.10.96::dcList/pro15/
}

main
