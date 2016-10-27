#!/bin/sh
log=/var/log/update_stat.log
echo "`date`: start update" >> $log
ts=`date +%s`
curl http://127.0.0.1:8010/api/v1/update
ts1=`date +%s`
delay=`echo "$ts1 - $ts" | bc`
echo "`date`: end update: $delay seconds"  >> $log
