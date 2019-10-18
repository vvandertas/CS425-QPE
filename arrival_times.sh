#!/usr/bin/env bash
echo "job arrival_time"
for f in $1-conf.json-run$2
do
	grep --no-group-separator 'Dispatched ' $f |awk '{split($0,a," "); print a[6] " " a[1] ":" a[2]}'
done

