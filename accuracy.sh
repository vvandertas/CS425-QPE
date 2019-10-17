for run in 1 2 3
do
	for f in $1-$run/*.out
	do
		grep 'Top1Accuracy' $f | tail -1 | cut -d',' -f3 | cut -d ':' -f2 | cut -d ')' -f1

	done
done
