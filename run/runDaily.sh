#!/bin/bash
baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/header.sh

function main() {
	##--统计------------------------
	sh $baseDir/pvuv/pvuvInfo.sh $yesterday
	sh $baseDir/article/articleInfo.sh $yesterday
    sh $baseDir/shareBack/shareBackInfo.sh $yesterday

	##--导入dc------------------------
	#toDc
	sh $baseDir/toDc.sh $yesterday
}
main
