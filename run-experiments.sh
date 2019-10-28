for configuration in exp?-conf.json 
do
  for run in 1 2 3
  do
    timeout 3800 ./src/sparkgen/sparkgen -r -d -o "outputbigdl/$configuration-run$run" -c $configuration 2>&1 | sudo tee "output/$configuration-run$run"
  done
done
