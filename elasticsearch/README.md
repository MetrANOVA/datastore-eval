# 1. Installation

## On the "inserter" machine

```
# clone repo
git clone git@github.com:MetrANOVA/datastore-eval.git

# change working directory to repo
cd datastore-eval/elasticsearch

# create virtualenv
python3 -m venv venv

#enter venv
. venv/bin/activate

# install requirements
pip install -r requirements.txt
```

# 2. Start Elasticsearch on remote machine

## On the "target" machine

You'll need to create ES target machines, similar to the `datastoreeval-small`, `datastoreeval-medium`, and `datastoreeval-large` machines. Use clickhouse machines for reference, maybe. They were created most recently.

Setup Notes:
- Make sure you use the correct SSD type: **"SSD persistent disk"** for the data directory for ES docker. This caused *very non-trivial error* while doing testing.
- Make sure you install the XFS tools packages for the OS.
- You'll have to format the SSD after spinning up the machine. We used XFS for all data volumes. For Ubuntu, the XFS commands are ass follows:
```
sudo -s
lsblk #get the id of the device (e.g sdb)
apt install xfsprogs #already installed on non-minimal
mkfs.xfs /dev/sdb
mkdir /mnt/data
mount -t xfs /dev/sdb /mnt/data
chmod -R 777 /mnt/data/ #make sure elastic can write to it
```
- You may or may not to want to add the mount to fstab. I didn't, but brayton did. I just don't recall how to do this in linux off the top of my head, so didn't bother.
- Commands to install docker on ubuntu:
```
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
apt update
apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
docker run hello-world
```
- I like to run an apache reverse proxy to kibana. Commands to setup apache:

```
apt install apache2
htpasswd -c /etc/apache2/.htpasswd metranova #create a user named metranova for kibana
cat << EOF | sudo tee /etc/apache2/conf-available/kibana.conf > /dev/null
<IfModule proxy_module>
    ProxyRequests Off
    <Proxy *>
        <IfVersion >= 2.4>
            Require all granted
        </IfVersion>
        <IfVersion < 2.4>
            Order deny,allow
            Allow from all
        </IfVersion>
    </Proxy>

    # The directives below apply to the root URL "/"
    <Location "/">
        AuthType Basic
        AuthName "Restricted Area"
        AuthUserFile /etc/apache2/.htpasswd
        Require valid-user

        # Proxy directives for the location
        ProxyPass http://localhost:5601/
        ProxyPassReverse http://localhost:5601/
        ProxyPreserveHost On
    </Location>
</IfModule>
EOF
a2enmod ssl proxy proxy_http
a2enconf kibana
a2ensite default-ssl
systemctl restart apache2
```


- Make sure you expose port 9200 on the docker container to the host,
- Make sure the host's port 9200 is public on its "google internal" (192.168.x.x) ip address
- Note down the "google internal" IP of the machine. This is the value you'll use for `$ES_HOST_IP` below

## Docker command

1. Copy the docker-compose.yml file provided to your system where you want to run elasticsearch.
2. Edit the file so that `ES_JAVA_OPTS=` sets the `-Xms` and `-Xmx` to 50% the system memory.
3. Run `docker compose up -d`
4. You should now have a running elastic instance on port 9200 of the internal network. You should also have a Kibana instance available on public interface port 443 via the Apache proxy. Login using the user/pass you setup when configuring Apache proxy. 

# 3. Run process harness

## On the "inserter" machine

```
export ES_HOST_IP=$(whatever you noted down previously)
export WORKER_COUNT=10 # or whatever worker count you're trying to sample
export OUTPUT_DIR=/home/andy/transformed_output # or similar

#create output dir
mkdir /home/andy/transformed_output

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