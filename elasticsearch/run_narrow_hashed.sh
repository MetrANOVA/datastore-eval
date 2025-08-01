WORKERS=$1
HOST=$2
TRANSFORMED_OUTPUT_DIR=$3

WORKERS_MINUS_ONE=$(($WORKERS-1))

LIMIT=100000000
PER_WORKER_LIMIT=$(($LIMIT / $WORKERS))

BATCH_SIZE=15000

source venv/bin/activate

# do table drop here

rm -R $TRANSFORMED_OUTPUT_DIR
mkdir -p $TRANSFORMED_OUTPUT_DIR

for i in `seq 0 1 $WORKERS_MINUS_ONE`;
do mkdir -p "$TRANSFORMED_OUTPUT_DIR/$i";
done;

for i in `seq 0 1 $WORKERS_MINUS_ONE`;
do echo $i;
python insert.py --infile /media/stardust-data/stardust_data-2025-03-28--2025-03-29.reversed.tsv --partition $i --total-partitions $WORKERS --limit $PER_WORKER_LIMIT --batch-size $BATCH_SIZE --transform-output-dir "$TRANSFORMED_OUTPUT_DIR/$i" --transform-output-intermediate &
done;

# wait for all background jobs to complete...
wait $(jobs -p)

# then loop over the worker input directories
for i in `seq 0 1 $WORKERS_MINUS_ONE`;
do echo $i;
python insert.py --host $HOST --total-partitions $WORKERS --partition $i --batch-size $BATCH_SIZE --transform-input-dir "$TRANSFORMED_OUTPUT_DIR/$i" &
sleep 0.1
done;

# wait for those jobs...
wait $(jobs -p)

python print_scoreboard.py --host $HOST --batch-size $BATCH_SIZE

