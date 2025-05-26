# sedb
Search Engine DataBase utils

![](https://img.shields.io/pypi/v/sedb?label=sedb&color=blue&cacheSeconds=60)

## Install

```sh
pip install sedb[common] --upgrade
```

Currently, `sedb` supports interacting with following services:

- common:
  - MongoDB
  - ElasticSearch
  - Redis
  - LLM REST API (OpenAI format)

- vector:
  - Milvus
  - Qdrant

You can install all dependencies by:

```sh
pip install sedb[all] --upgrade
```

or default extreme light-weight dependencies by:

```sh
pip install sedb --upgrade
```

## Usage

Run example:

```sh
python example.py
```

See: [example.py](./example.py)

```python
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

import sedb

from sedb import MongoOperator, MongoConfigsType


if __name__ == "__main__":
    mongo_configs = {
        "host": "localhost",
        "port": 27017,
        "dbname": "test",
    }

    collection = "videos"
    mongo = MongoOperator(configs=mongo_configs, indent=0)
    cursor1 = mongo.get_cursor(
        collection,
        filter_index="pubdate",
        filter_op="lte",
        filter_range="2012-01-01",
        sort_index="pubdate",
        sort_order="asc",
    )
    print(cursor1.next())
    cursor2 = mongo.get_cursor(
        collection,
        filter_index="pubdate",
        filter_op="range",
        filter_range=["2012-12-31", "2012-01-01"],
        sort_index="pubdate",
        sort_order="asc",
    )
    print(cursor2.next())
    cursor3 = mongo.get_cursor(
        collection,
        filter_index="pubdate",
        filter_op="range",
        filter_range=["2012-01-01", None],
        sort_index="pubdate",
        sort_order="asc",
    )
    print(cursor3.next())
```