#!/usr/bin/env bash
for f in $1-conf.json-run*
do
	grep --no-group-separator 'Dispatched ' $f |awk '{split($0,a," "); print a[6] " " a[1] " " a[2]}'
done

