import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from tclogger import logger, logstr, get_now_str, dict_to_str

from sedb import MongoOperator, MongoConfigsType
from sedb import RocksOperator, RocksConfigsType
from sedb import filter_str_to_params, filters_str_to_mongo_filters


def test_mongo():
    mongo_configs: MongoConfigsType = {
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
    rocks_configs: RocksConfigsType = {"db_path": "z.rkdb"}
    rocks = RocksOperator(configs=rocks_configs)
    rocks.db.put("now", get_now_str())
    print("  * now:", rocks.db.key_may_exist("now"), rocks.db.get("now"))


def test_filters_str_to_mongo_filter():
    filters_str = (
        "d:pubdate<=2012-01-01;insert_at=[2024-01-01,2024-12-31];u:stat.view>1kw"
    )
    filter_strs = filters_str.split(";")
    for filter_str in filter_strs:
        logger.note(f"* filter_str: {logstr.mesg(filter_str)}")
        filter_params = filter_str_to_params(filter_str)
        logger.okay(dict_to_str(filter_params, add_quotes=True), indent=2)


if __name__ == "__main__":
    # test_mongo()
    # test_rocks()
    test_filters_str_to_mongo_filter()

    # python example.py
