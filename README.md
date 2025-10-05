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
from tclogger import logger, logstr, get_now_str, dict_to_str

from sedb import MongoOperator, MongoConfigsType
from sedb import RocksOperator, RocksConfigsType
from sedb import filter_str_to_params, filters_str_to_mongo_filter
from sedb import MongoDocsGenerator


def test_rocks():
    rocks_configs: RocksConfigsType = {"db_path": "z.rkdb"}
    rocks = RocksOperator(configs=rocks_configs)
    rocks.db.put("now", get_now_str())
    print("  * now:", rocks.db.key_may_exist("now"), rocks.db.get("now"))


def test_filter_str_to_params(filters_str: str):
    filter_strs = filters_str.split(";")
    for filter_str in filter_strs:
        logger.note(f"* filter_str: {logstr.mesg(filter_str)}")
        filter_params = filter_str_to_params(filter_str)
        logger.okay(dict_to_str(filter_params, add_quotes=True), indent=2)


def test_filters_str_to_mongo_filter(filters_str: str):
    logger.note(f"filters_str: {logstr.mesg(filters_str)}")
    mongo_filter = filters_str_to_mongo_filter(filters_str)
    logger.okay(dict_to_str(mongo_filter, add_quotes=True), indent=2)


def test_mongo_generator():
    generator = MongoDocsGenerator()
    generator.init_all_with_cli_args()
    for docs_batch in generator.docs_batch_generator():
        for doc in docs_batch:
            logger.mesg(doc)
    # python example.py -H localhost -P 27017 -D <DBNAME> -c <COLLECTION> -i pubdate -s 2025-10-06 -x "u:stat.view>1k" -fi "title,pubdate,stat.view" -m 5 -b 1000


if __name__ == "__main__":
    # test_rocks()
    # filters_str = "d:pubdate<=2012-01-01;insert_at=[2024-12-01,2024-07-01];u:stat.view>1kw;index_at=[2023-01-01,None]"
    # test_filter_str_to_params(filters_str)
    # test_filters_str_to_mongo_filter(filters_str)
    test_mongo_generator()

    # python example.py

```