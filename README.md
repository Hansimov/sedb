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
  - RocksDB

- vector:
  - Faiss
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