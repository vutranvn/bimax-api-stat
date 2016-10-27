#!/bin/sh
log=/var/log/update_stat.log
end="$1"
dend=`date -d "$end" +%s`
start="$2"
dstart=`date -d "$start" +%s`
step=$3
( while [ $dend -ge $dstart ];do
	dd1="`date -d@$dend +"%Y-%m-%d"`"
	dd2="`date -d@$dend +"%H:%M"`"
	dd3="`date -d@$dend +"%M"`"
	ts=`date +%s`
	curl -s "http://127.0.0.1:8010/api/v1/update?date=$dd1%20$dd2&unit=minute"  > /dev/null
	if [ "$dd3" = "00" ];then
		curl -s "http://127.0.0.1:8010/api/v1/update?date=$dd1%20$dd2&unit=hour,day,month,year"  > /dev/null
		ts1=`date +%s`
		delay=`echo "$ts1 - $ts" | bc`
		echo "hour: $dd1 $dd2 $delay"
	fi
	echo
	ts1=`date +%s`
	delay=`echo "$ts1 - $ts" | bc`
	echo "min:$dd1 $dd2 $delay"
	dend=$((dend - step))
done ) >> $log
