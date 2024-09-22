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
    mongo = MongoOperator(configs=mongo_configs)

    # python example.py
