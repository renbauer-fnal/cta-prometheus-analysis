if ! command -v promtool &> /dev/null
then
	echo 'promtool command not found! Ensure promtool is in $PATH'
	exit 1
fi

start=1692172800; while [ $start -lt $(date +%s) ]; do
  echo $start
  end=$(($start + 11000))
  promtool query range --start $start --end $end --step 1s http://storagedev201.fnal.gov:9090 "bytes_transfered_in_session{drive_status='TRANSFERING'}" > prom_dump/transfered_bytes.$start
  promtool query range --start $start --end $end --step 1s http://storagedev201.fnal.gov:9090 "session_elapsed_time{drive_status='TRANSFERING'}" > prom_dump/elapsed_time.$start
  start=$end
done
