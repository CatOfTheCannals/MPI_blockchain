#!/usr/bin/env bash
rm info.log
rm error.log
for i in {1..100}
do 
	make run >> info.log 2>> error.log
	echo Corrida $i 
	echo 
done