#!/bin/bash

# joey vigil & charan nemarugommula
# project eureka and project airwise backend

# this script checks to see if there is a postgresql database called
# eureka and then checks if it contains a table called airwise_data.
# if it doesn't exist yet, it will make it and format it to receive
# data packets from AIRWISE v1 hardware. the user must pass their
# psql name and password (in that order) as command line arguments
# to this program.

USERNAME=$1
export PGPASSWORD=$2


if psql -lqt | cut -d \| -f 1 | grep -qw eureka; then
	echo PSQL database EUREKA exists, checking on TABLES next...
else
	echo PSQL database EUREKA not found. Creating...
	psql -U $USERNAME -c "CREATE DATABASE eureka;"
	if [ $? -ne 0 ]; then
		echo Failed to create database EUREKA. Exiting...
		exit 1
	fi
fi

if psql -U $USERNAME -d eureka -c "\d" | cut -d \| -f 2 | grep -qw airwise_data; then
	echo Table airwise_data exists. Exiting...
	exit 0
else
	echo Table airwise_data not found. Creating...
	psql -U $USERNAME -d eureka -c "CREATE TABLE airwise_data(node BIGINT, topic_id TEXT,longname TEXT,pressure REAL,gas REAL,iaq INTEGER,humidity REAL,temperature REAL,timestamp_node BIGINT, pst_time text);"

	if [ $? -ne 0 ]; then
		echo Failed to create table airwise_data. Exiting...
		exit 1
	else
		echo Table airwise_data created. Ready to receive data. Exiting...
	fi
fi





if psql -U $USERNAME -d eureka -c "\d" | cut -d \| -f 2 | grep -qw battery_data; then
	echo Table battery_data exists. Exiting...
	exit 0
else
	echo Table battery_data not found. Creating...
    psql -U $USERNAME -d eureka -c "CREATE TABLE battery_data(node BIGINT, topic_id TEXT,longname TEXT,voltage NUMERIC, battery_level NUMERIC, pst_time text);"

	if [ $? -ne 0 ]; then
		echo Failed to create table battery_data. Exiting...
		exit 1
	else
		echo Table battery_data created. Ready to receive data. Exiting...
	fi
fi

exit 0