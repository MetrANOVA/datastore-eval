# Insert and Query Benchmark scripts for TimescaleDB

## Installation

```
# create a virtual environment
python3 -m venv venv

# activate the virtual environment
source venv/bin/activate

# install requirements
pip install -r requirements.txt
```

## Parse stardust-wide data, assemble metadata to foreign table and COPY

```
# Insert stardust data from /media/data/stardust-2025-03-01--2025-03-02.wide.tsv
python insert.py --format stardust-wide --strategy hashed-metadata /media/data/stardust-2025-03-01--2025-03-02.wide.tsv
```

## Parse stardust-narrow data, assemble metadata to foreign table and COPY

```
python insert.py --format stardust-narrow --strategy hashed-metadata /media/data/stardust-2025-03-28--2025-03-29.tsv
```

## Parse stardust-narrow data, assemble metadata to column in measurement table and COPY

```
python insert.py --format stardust-narrow --strategy inline-metadata /media/data/stardust-2025-03-28--2025-03-29.tsv
```

