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
python insert.py --values-table values_wide_inline_normalized --strategy inline-metadata --host $HOST --infile /media/stardust-data-wide/stardust_data-2025-03-11--2025-03-13.wide.reversed.tsv --wide --normalized --offset $i --skip $WORKERS --limit $PER_WORKER_LIMIT --batch-size 5000 --binary-output-dir "$BINARY_OUTPUT_DIR/$i" --binary-output-intermediate &
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
python insert.py --values-table values_wide_inline_normalized --strategy inline-metadata --host $HOST --infile /media/stardust-data-wide/stardust_data-2025-03-11--2025-03-13.wide.reversed.tsv --wide --normalized --offset $i --skip $WORKERS --limit $PER_WORKER_LIMIT --batch-size 5000 --binary-input-dir "$BINARY_OUTPUT_DIR/$i" &
sleep 0.1
done;

wait $(jobs -p)

bash get_results.sh $HOST 30
