# Stardust Public Data Fetcher

## Installation

```
# create a virtual environment
python3 -m venv venv

# activate the virtual environment
source venv/bin/activate

# install requirements
pip install -r requirements.txt
```

## Fetching data

```
# fetch data from january-march 2025
python stardust_fetcher.py --begin 2025-01-01 --end 2025-03-01 --format tsv > data_dump-2025-01--20250-03.tsv
```


