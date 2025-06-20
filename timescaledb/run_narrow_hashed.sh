WORKERS=$1
HOST=$2
BINARY_OUTPUT_DIR=$3

WORKERS_MINUS_ONE=$(($WORKERS-1))

LIMIT=100000000
PER_WORKER_LIMIT=$(($LIMIT / $WORKERS))

source venv/bin/activate

cat drop_tables.sql | psql --host $HOST --user timescale
cat create_tables.sql | psql --host $HOST --user timescale
# cat it again, there's some kind of small bug, not worth fixing
cat create_tables.sql | psql --host $HOST --user timescale

rm -R $BINARY_OUTPUT_DIR
mkdir -p $BINARY_OUTPUT_DIR

for i in `seq 0 1 $WORKERS_MINUS_ONE`;
do mkdir -p "$BINARY_OUTPUT_DIR/$i";
done;

for i in `seq 0 1 $WORKERS_MINUS_ONE`;
do echo $i;
python insert.py --values-table values_inline --strategy inline-metadata --host $HOST --infile /media/stardust-data/stardust_data-2025-03-28--2025-03-29.reversed.tsv --total-partitions=$WORKERS --partition=$i --limit $PER_WORKER_LIMIT --batch-size 10000 --binary-output-dir "$BINARY_OUTPUT_DIR/$i" --binary-output-intermediate &
sleep 0.1
done;

# sleep for a long time while we do the prep work for the batches and inserts
wait $(jobs -p)

cat drop_tables.sql | psql --host $HOST --user timescale
cat create_tables.sql | psql --host $HOST --user timescale
# cat it again, there's some kind of small bug, not worth fixing
cat create_tables.sql | psql --host $HOST --user timescale

for i in `seq 0 1 $WORKERS_MINUS_ONE`;
do echo $i;
python insert.py --values-table values_inline --strategy inline-metadata --host $HOST --infile /media/stardust-data/stardust_data-2025-03-28--2025-03-29.reversed.tsv --total-partitions=$WORKERS --partition=$i --limit $PER_WORKER_LIMIT --batch-size 10000 --binary-input-dir "$BINARY_OUTPUT_DIR/$i" &
sleep 0.1
done;

wait $(jobs -p)

bash get_results.sh $HOST 30
