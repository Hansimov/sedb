import re
from typing import Union
from tclogger import logger, unify_ts_and_str, str_to_ts

from .mongo_types import FilterIndexType, FilterOpType, FilterRangeType, SortOrderType
from .mongo_types import (
    MongoCursorParamsType,
    MongoCountParamsType,
    MongoFilterParamsType,
)
from .mongo_types import COUNT_ARG_KEYS, FILTER_ARG_KEYS, DATE_FIELDS


def range_to_mongo_filter_and_sort_info(
    filter_index: FilterIndexType = None,
    start_val: Union[str, int, None] = None,
    end_val: Union[str, int, None] = None,
    sort_index: FilterIndexType = None,
    sort_order: SortOrderType = "asc",
    is_date_field: bool = None,
) -> tuple[dict, dict]:
    filter_op = None
    filter_range = None
    filter_dict = {}

    if is_date_field is True:
        start_val, _ = unify_ts_and_str(start_val)
        end_val, _ = unify_ts_and_str(end_val)

    if filter_index:
        if start_val is not None and end_val is not None:
            filter_op = "range"
            filter_range = [start_val, end_val]
            filter_dict = {filter_index: {"$gte": start_val, "$lte": end_val}}
        elif start_val is not None:
            filter_op = "gte"
            filter_range = start_val
            filter_dict = {filter_index: {"$gte": start_val}}
        elif end_val is not None:
            filter_op = "lte"
            filter_range = end_val
            filter_dict = {filter_index: {"$lte": end_val}}
        else:
            pass

    filter_info = {
        "index": filter_index,
        "op": filter_op,
        "range": filter_range,
        "dict": filter_dict,
    }
    sort_info = {"index": sort_index, "order": sort_order}
    return filter_info, sort_info


def filter_params_to_mongo_filter(
    filter_index: FilterIndexType = None,
    filter_op: FilterOpType = "gte",
    filter_range: FilterRangeType = None,
    date_fields: list[str] = DATE_FIELDS,
    is_date_field: bool = None,
) -> dict:
    filter_dict = {}
    if filter_index:
        if filter_op == "range":
            if (
                filter_range
                and isinstance(filter_range, (tuple, list))
                and len(filter_range) == 2
            ):
                l_val, r_val = filter_range
                if is_date_field is True or filter_index.lower() in date_fields:
                    if isinstance(l_val, str):
                        l_val = str_to_ts(l_val)
                    if isinstance(r_val, str):
                        r_val = str_to_ts(r_val)
                if l_val is not None and r_val is not None:
                    filter_dict[filter_index] = {
                        "$lte": max([l_val, r_val]),
                        "$gte": min([l_val, r_val]),
                    }
                elif l_val is not None:
                    filter_dict[filter_index] = {"$gte": l_val}
                elif r_val is not None:
                    filter_dict[filter_index] = {"$lte": r_val}
                else:
                    pass
            else:
                raise ValueError(f"× Invalid filter_range: {filter_range}")
        elif filter_op in ["gt", "lt", "gte", "lte"]:
            if filter_range and isinstance(filter_range, (int, float, str)):
                if filter_index.lower() in date_fields:
                    if isinstance(filter_range, str):
                        filter_range = str_to_ts(filter_range)
                filter_dict[filter_index] = {f"${filter_op}": filter_range}
            else:
                raise ValueError(f"× Invalid filter_range: {filter_range}")
        else:
            raise ValueError(f"× Invalid filter_op: {filter_op}")
    return filter_dict


SYM_OP_MAP = {">": "gt", "<": "lt", ">=": "gte", "<=": "lte", "=": "eq"}

RE_FLAGS = r"(?P<flags>[a-zA-Z]+:)?"
RE_FIELD = r"(?P<field>[^=><]+)"
RE_SYM = r"(?P<sym>=|>=|<=|>|<)"
RE_RANGE = r"(?P<range>\[[^;\]]+\])"
RE_VALUE = r"(?P<value>[^;\[\]]+)"
RE_FILTER = rf"{RE_FLAGS}{RE_FIELD}{RE_SYM}({RE_RANGE}|{RE_VALUE})"

UNIT_NUM_MAP = {"k": 1000, "w": 10000, "m": 1000000}
RE_UNIT_NUM = r"^(?P<num>[\d.]+)(?P<units>[kKwWmM]*)$"


def num_unit_str_to_int(s: str) -> int:
    match = re.match(RE_UNIT_NUM, s)
    if not match:
        raise ValueError(f"× Invalid number-unit string: {s}")
    num = int(match.group("num"))
    units = match.group("units")
    if not units:
        return num
    for unit in units.lower():
        num *= UNIT_NUM_MAP.get(unit, 1)
    return num


def unify_range_value_str(
    range_str: str = None, value_str: str = None, flags: str = None
) -> tuple:
    if range_str:
        range_str = range_str.strip("[]")
        rvs = [s.strip() for s in range_str.split(",")]
        if len(rvs) != 2:
            raise ValueError(f"× Invalid range_str: {range_str}")
        rvs = [rv if rv.lower() != "none" else None for rv in rvs]
    elif value_str:
        rvs = value_str.strip()
        if rvs.lower() == "none":
            rvs = None
    else:
        raise ValueError("× Must provide either range or value!")

    if not flags:
        return rvs, None

    flags = flags.strip(":").lower()
    is_date_field = None
    if "d" in flags:
        is_date_field = True

    if not isinstance(rvs, list):
        rvs = [rvs]

    urvs = []
    for rv in rvs:
        if rv is None:
            urvs.append(rv)
            continue
        if "i" in flags:
            urvs.append(int(rv))
            continue
        if "f" in flags:
            urvs.append(float(rv))
            continue
        if "u" in flags:
            urvs.append(num_unit_str_to_int(rv))
            continue
        if "s" in flags:
            urvs.append(rv)
            continue
        if "b" in flags:
            if rv.lower() in ["1", "true", "yes"]:
                urvs.append(True)
            elif rv.lower() in ["0", "false", "no"]:
                urvs.append(False)
            else:
                raise ValueError(f"× Invalid bool value: {rv}")
            continue
        urvs.append(rv)

    if len(urvs) == 1:
        res_rvs = urvs[0]
    else:
        res_rvs = urvs

    return res_rvs, is_date_field


def filter_str_to_params(filter_str: str) -> MongoFilterParamsType:
    """`<flags>:<field><sym><value/range>`

    Flags:
        - d: type date
        - b: type bool
        - i: type int
        - f: type float
        - u: type int-unit
        - s: type str (default)

    Examples:

    * `insert_at>=2023-01-01`:

    ```
    {
        "filter_index": "insert_at",
        "filter_op": "gte",
        "filter_range": "2023-01-01",
        "is_date_field": True
    }
    ```

    * `d:pubdate=[2023-01-01,2023-06-01]`:

    ```
    {
        "filter_index": "pubdate",
        "filter_op": "range",
        "filter_range": ["2023-01-01", "2023-06-01"],
        "is_date_field": True
    }
    ```

    * `u:stat.view>=10k`:

    ```
    {
        "filter_index": "stat.view",
        "filter_op": "gte",
        "filter_range": 10000,
        "is_date_field": False
    }
    ```

    """
    match = re.match(RE_FILTER, filter_str.strip())
    if not match:
        return {}

    try:
        filter_index = match.group("field").strip()
        sym = match.group("sym").strip()
        filter_op = SYM_OP_MAP.get(sym, "gte")
        range_str = match.group("range")
        value_str = match.group("value")
        if filter_op == "eq" and range_str:
            filter_op = "range"
        flags = match.group("flags")
        filter_range, is_date_field = unify_range_value_str(
            range_str=range_str, value_str=value_str, flags=flags
        )
    except Exception as e:
        logger.warn(f"× Failed to parse `{filter_str}`: ({e})")
        return {}

    return {
        "filter_index": filter_index,
        "filter_op": filter_op,
        "filter_range": filter_range,
        "is_date_field": is_date_field,
    }


def filters_str_to_mongo_filter(filters_str: str, sep: str = ";") -> dict:
    if not filters_str:
        return {}
    res_dict = {}
    filter_strs = filters_str.split(sep)
    for filter_str in filter_strs:
        filter_params = filter_str_to_params(filter_str)
        filter_dict = filter_params_to_mongo_filter(**filter_params)
        res_dict.update(filter_dict)
    return res_dict


def update_mongo_filter(
    filter_dict: dict, extra_filters: Union[dict, list[dict]] = None
) -> dict:
    if extra_filters:
        if isinstance(extra_filters, dict):
            filter_dict.update(extra_filters)
        else:
            for extra_filter in extra_filters:
                filter_dict.update(extra_filter)
    return filter_dict


def extract_count_params_from_cursor_params(
    cursor_params: MongoCursorParamsType,
) -> MongoCountParamsType:
    return {key: cursor_params.get(key) for key in COUNT_ARG_KEYS}


def extract_filter_params_from_cursor_params(
    cursor_params: MongoCursorParamsType,
) -> MongoFilterParamsType:
    return {key: cursor_params.get(key) for key in FILTER_ARG_KEYS}
