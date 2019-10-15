#!/bin/bash

DIR=part1/c/
FILE=WordCount
CP="./bin:/usr/lib/hadoop/*:/usr/lib/hadoop/lib/*:/usr/lib/hadoop/client-0.20/*"

rm -f bin/${DIR}*.class  bin/${FILE}.jar

javac -cp $CP src/${DIR}${FILE}.java -d bin
cd bin;jar cfv ${FILE}.jar ${DIR}*.class;cd -

if [ "$1" == "local" ]; then
	java -cp $CP ${DIR}${FILE} input/${DIR} output
	cat output/*
	exit 0
fi;

HDIR=/usr/cloudera

hadoop fs -mkdir ${HDIR}/input
hadoop fs -copyFromLocal input/${DIR}*.txt ${HDIR}/input
hadoop jar bin/${FILE}.jar ${DIR}${FILE} ${HDIR}/input ${HDIR}/output
echo "=================================================="
hadoop fs -cat ${HDIR}/output/*
