#!/usr/bin/env bash
echo "job service_time end_time"
for f in $1-conf.json-run$2
do
	grep --no-group-separator 'LOG-' $f -A1 |grep --no-group-separator -v 'LOG'|awk '{split($0,a,","); print a[2] " " a[6]-a[5] " " a[6]}'
done