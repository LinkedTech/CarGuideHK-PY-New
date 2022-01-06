#!/bin/bash


cd /opt/app/CarGuideHK/python-scripts/CarGuideHK-AdminPY

COMMAND='2'  # copy list tables 

check=$(ps -ef | grep  copyDataFromPD2TE.py | grep -v grep | wc -l)
#Restart the process if it is dead
if [ $check -eq 0 ];
then
	echo "Cannot find it"
	python3 ./CopyDataFromPD2TE/copyDataFromPD2TE.py 1
else
	echo "Found it"
fi

