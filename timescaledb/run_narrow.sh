WORKERS=$1
HOST=$2
BINARY_OUTPUT_DIR=$3

WORKERS_MINUS_ONE=$(($WORKERS-1))

LIMIT=10000 #0000
PER_WORKER_LIMIT=$(($LIMIT / $WORKERS))

cat drop_tables.sql | psql --host $HOST --user timescale
cat create_tables.sql | psql --host $HOST --user timescale
# cat it again, there's some kind of small bug, not worth fixing
cat create_tables.sql | psql --host $HOST --user timescale

mkdir -p $BINARY_OUTPUT_DIR

for i in `seq 0 1 $WORKERS_MINUS_ONE`;
do python insert.py --host $HOST --infile /media/stardust-data/stardust_data-2025-03-28--2025-03-29.reversed.tsv --offset $WORKERS_MINUS_ONE --limit $PER_WORKER_LIMIT --batch-size 10000 &
done;

rm -R $BINARY_OUTPUT_DIR
