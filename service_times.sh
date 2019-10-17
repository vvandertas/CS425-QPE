for f in $1-conf.json-run*
do
	grep --no-group-separator 'LOG-' $f -A1|grep --no-group-separator -v 'LOG'|awk '{split($0,a,","); print a[6]-a[5]}'
done

