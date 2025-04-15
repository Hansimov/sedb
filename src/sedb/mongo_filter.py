from typing import Literal, Union
from tclogger import unify_ts_and_str, str_to_ts

COUNT_ARG_KEYS = [
    "collection",
    "filter_index",
    "filter_op",
    "filter_range",
    "extra_filters",
    "estimate_count",
]
FILTER_ARG_KEYS = ["filter_index", "filter_op", "filter_range", "is_date_index"]


def range_to_mongo_filter_and_sort_info(
    filter_index: str = None,
    start_val: Union[str, int, None] = None,
    end_val: Union[str, int, None] = None,
    sort_index: str = None,
    sort_order: Literal["asc", "desc"] = "asc",
    is_date_index: bool = None,
) -> tuple[dict, dict]:
    filter_op = None
    filter_range = None
    filter_dict = {}

    if is_date_index is True:
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


def to_mongo_filter(
    filter_index: str = None,
    filter_op: Literal["gt", "lt", "gte", "lte", "range"] = "gte",
    filter_range: Union[int, str, tuple, list] = None,
    date_fields: list[str] = ["pubdate", "insert_at", "index_at"],
    is_date_index: bool = None,
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
                if is_date_index is True or filter_index.lower() in date_fields:
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


def update_filter(
    filter_dict: dict, extra_filters: Union[dict, list[dict]] = None
) -> dict:
    if extra_filters:
        if isinstance(extra_filters, dict):
            filter_dict.update(extra_filters)
        else:
            for extra_filter in extra_filters:
                filter_dict.update(extra_filter)
    return filter_dict


def extract_count_params_from_cursor_params(cursor_params: dict) -> dict:
    return {key: cursor_params.get(key) for key in COUNT_ARG_KEYS}


def extract_filter_params_from_cursor_params(cursor_params: dict) -> dict:
    return {key: cursor_params.get(key) for key in FILTER_ARG_KEYS}
