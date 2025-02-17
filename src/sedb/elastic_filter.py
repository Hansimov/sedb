from typing import Union


def to_elastic_filter(
    ids: list[str, int] = None,
    id_field: str = None,
    must_have_fields: list[str] = None,
    exprs: Union[dict, list[dict]] = None,
    output_fields: list[str] = None,
) -> dict:
    res = {}
    must_exprs = []
    if ids:
        if id_field is None:
            ids_filter = {"ids": {"values": ids}}
        else:
            ids_filter = {"terms": {id_field: ids}}
        must_exprs.append(ids_filter)
    if must_have_fields:
        must_have_filters = [{"exists": {"field": field}} for field in must_have_fields]
        must_exprs.extend(must_have_filters)
    if exprs:
        if isinstance(exprs, dict):
            exprs = [exprs]
        must_exprs.extend(exprs)

    if must_exprs:
        res["query"] = {"bool": {"filter": must_exprs}}
    if output_fields:
        res["_source"] = output_fields
    else:
        res["_source"] = False
        res["stored_fields"] = []

    if res:
        other_params = {
            "track_total_hits": True,
            "track_scores": False,
            "size": len(ids),
        }
        res.update(other_params)

    return res
