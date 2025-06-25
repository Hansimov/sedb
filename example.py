import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from sedb import MongoOperator, MongoConfigsType
from sedb import RocksOperator, RocksConfigsType


def test_mongo():
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


def test_rocks():
    rocks_configs = {
        "db_path": "XXXXX/rocksdb",
    }

    rocks = RocksOperator(configs=rocks_configs)


if __name__ == "__main__":
    # test_mongo()
    test_rocks()

    # python example.py
