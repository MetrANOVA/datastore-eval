WORKERS=$1
HOST=$2
CSV_OUTPUT_DIR=$3
SPLIT=$4

WORKERS_MINUS_ONE=$(($WORKERS-1))

LIMIT=100000000

if [ -z "$SPLIT" ]; then
    
    rm -R $CSV_OUTPUT_DIR/*
    mkdir -p $CSV_OUTPUT_DIR

    for i in `seq 0 1 $WORKERS_MINUS_ONE`;
    do mkdir -p "$BINARY_OUTPUT_DIR/$i";
    done;

    python insert.py --infile /media/stardust-data/stardust_data-2025-03-11--2025-03-13.wide.reversed.tsv --output-dir=/media/tmpdata/victoria/splits/ --batch-size 10000 --split --limit $LIMIT
fi
    
for i in `seq 0 1 $WORKERS_MINUS_ONE`;
do echo $i;
   python insert.py --host=$HOST --port=443 --output-dir=/media/tmpdata/victoria/splits/1 --workers=$WORKERS --worker=$i --wide --insert --batch-size 10000 &
sleep 0.1
done;

wait $(jobs -p)


