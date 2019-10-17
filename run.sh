#!/bin/bash

if [[ $# -lt 2 ]]; then
	echo "Usage  : $0 <package_dir> <class_name>"
	echo "Example: $0 part1/c WordCount"
        exit 1
fi;

if [ -z "$HADOOP_LIB_DIR" ]
then
        HADOOP_LIB_DIR=/usr/lib/hadoop
        echo "\$HADOOP_LIB_DIR is not set, use default: $HADOOP_LIB_DIR"
        echo "If this not correct, you must run cmd HADOOP_LIB_DIR=</path/to/lib/hadoop>, then rerun $0"
else
        echo "\$HADOOP_LIB_DIR is $HADOOP_LIB_DIR"
fi

DIR=$1/
FILE=$2
CP="./bin:${HADOOP_LIB_DIR}/*:${HADOOP_LIB_DIR}/lib/*:${HADOOP_LIB_DIR}/client-0.20/*"

rm -f bin/${DIR}*.class  bin/${FILE}.jar
mkdir bin

javac -cp $CP src/${DIR}${FILE}.java -d bin
cd bin;jar cfv ${FILE}.jar ${DIR}*.class;cd -

if [ "$3" == "local" ]; then
	java -cp $CP ${DIR}${FILE} input/${DIR} output
	cat output/*
	exit 0
fi;

HDIR=/user/cloudera

hadoop fs -rm ${HDIR}/input/*
hadoop fs -rmdir ${HDIR}/input
hadoop fs -mkdir ${HDIR}/input
hadoop fs -copyFromLocal input/${DIR}*.txt ${HDIR}/input
hadoop jar bin/${FILE}.jar ${DIR}${FILE} ${HDIR}/input ${HDIR}/output $3
echo "=================================================="
echo "hadoop fs -cat ${HDIR}/input/*"
echo "=================================================="
hadoop fs -cat ${HDIR}/input/*
echo -e "\n=================================================="
echo "hadoop fs -cat ${HDIR}/output/*"
echo "=================================================="
#hadoop fs -cat ${HDIR}/output/*
files=`hadoop fs -ls -C ${HDIR}/output`
set -f                      # avoid globbing (expansion of *).
array=(${files// / })

for i in "${array[@]}"
do
   echo "==>$i<=="
   hadoop fs -cat "$i"
done
