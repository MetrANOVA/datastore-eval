HOST=$1
SECONDS=$2

echo "SELECT time_bucket('$SECONDS seconds', start_time) AS bucket, sum(batch_size) / $SECONDS as inserts_per_second from scoreboard group by bucket order by bucket;" | psql --host $HOST --user timescale
