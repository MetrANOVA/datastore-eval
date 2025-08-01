# 1. Installation

## On the "inserter" machine

```
# clone repo
git clone git@github.com:MetrANOVA/datastore-eval.git

# change working directory to repo
cd datastore-eval/elasticsearch

# create virtualenv
python3 -m venv venv

# install requirements
pip install -r requirements.txt
```

# 2. Start Elasticsearch on remote machine

## On the "target" machine

You'll need to create ES target machines, similar to the `datastoreeval-small`, `datastoreeval-medium`, and `datastoreeval-large` machines. Use clickhouse machines for reference, maybe. They were created most recently.

Setup Notes:
- Make sure you use the correct SSD type: **"SSD persistent disk"** for the data directory for ES docker. This caused *very non-trivial error* while doing testing.
- Make sure you install the XFS tools packages for the OS.
- You'll have to format the SSD after spinning up the machine. We used XFS for all data volumes. Looking up the correct formatting commands for XFS is a PITA. Sorry.
- You may or may not to want to add the mount to fstab. I didn't, but brayton did. I just don't recall how to do this in linux off the top of my head, so didn't bother.
- Make sure you expose port 9200 on the docker container to the host,
- Make sure the host's port 9200 is public on its "google internal" (192.168.x.x) ip address
- Note down the "google internal" IP of the machine. This is the value you'll use for `$ES_HOST_IP` below

## Docker command

```
docker run -e discovery.type="single-node" -e ES_JAVA_OPTS="-Xmx750m -Xms750m" -e ELASTIC_USERNAME="elastic" -e ELASTIC_PASSWORD="DkIedPPSCb"  -p 9200:9200 -e xpack.security.enabled="false" --name es elasticsearch:9.0.2
```

N.B. This docker command doesn't specify the mount for the SSD data directory, that exercise is "left to the reader" sorry.

(if you need to restart it, don't forget to `docker rm es` first)

# 3. Run process harness

## On the "inserter" machine

```
export ES_HOST_IP=$(whatever you noted down previously)
export WORKER_COUNT=10 # or whatever worker count you're trying to sample
export OUTPUT_DIR=/home/andy/transformed-output # or similar

cd datastore-eval/elasticsearch
bash run_narrow.sh $WORKER_COUNT $ES_HOST_IP $OUTPUT_DIR
```

the `run_narrow` script will:
- activate the virtualenv (assuming `venv/bin/activate`)
- transform the output for the "narrow" file into appropriate slices per worker
- run a bunch of worker processes to actually perform the inserts from the transformed files
- run the "print_scoreboard" script to show the insert rate per second from the DB's point of view, in one-minute buckets

# 4. Run the other process harnesses

## On the "inserter" machine

```
bash run_narrow_hashed.sh $WORKER_COUNT $ES_HOST_IP $OUTPUT_DIR    # hashed approach because this was better for mongo...
bash run_wide_hashed.sh $WORKER_COUNT $ES_HOST_IP $OUTPUT_DIR      # wide format data
bash run_narrow_hashed.sh $WORKER_COUNT $ES_HOST_IP $OUTPUT_DIR    # wide format data with hashed approach because this was better for mongo...
```

# 5. Repeat steps 3 and 4 to fill in the sample grid

here: https://docs.google.com/spreadsheets/d/1pJtv2nNxTr7vmKj5batTJV2tThS0GZ_mDW_rOiIasOk/edit?gid=0#gid=0

# 6. Efficiency optimizations

## Human time-savings during insert

This process is very time-consuming.

I found it was best if I did all of the "sliced data" and then ran that set of "sliced data" against the different worker variations.

that is:

- slice data for 10 workers against "large" instance
- comment out slicing code in process harness
- run the same sliced data against "medium" instance
- run the same sliced data against "small" instance

I realize that's pretty manual, but it's *really time consuming*. You might want to be more fancy and add a command line switch or something, or just have "run narrow" tests all hosts sequentially, IDK.

## Human time-savings for sampling

I found that it was best to start with 10 workers, then do 16, and then 4 to see if things changed dramatically. If both are less efficient than 10 workers, 10 is probably close enough to the correct target number.

Use a manual binary-search approach to find the "most efficient" number of workers. It will save a very non-trivial amount of time.