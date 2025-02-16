from typing import Union


def to_mongo_projection(
    include_fields: list[str] = None, exclude_fields: list[str] = None
) -> dict:
    if include_fields:
        projection = {field: 1 for field in include_fields}
    elif exclude_fields:
        projection = {field: 0 for field in exclude_fields}
    else:
        projection = None
    return projection


def to_mongo_pipeline(
    local_collection: str,
    foreign_collection: str,
    local_id_field: str,
    foreign_id_field: str,
    local_fields: list[str],
    foreign_fields: list[str],
    must_in_local_ids: list[Union[str, int]] = None,
    must_in_foreign_ids: list[Union[str, int]] = None,
    must_have_local_fields: list[str] = None,
    must_have_foreign_fields: list[str] = None,
    local_filter_dict: dict = None,
    foreign_filter_dict: dict = None,
    as_name: str = None,
) -> list[dict]:
    pipeline = []

    if must_in_local_ids:
        match_expr = {"$match": {local_id_field: {"$in": must_in_local_ids}}}
        pipeline.append(match_expr)
    if must_have_local_fields:
        if isinstance(must_have_local_fields, str):
            must_have_local_fields = [must_have_local_fields]
        match_expr = {
            "$match": {
                f"{local_collection}.{field}": {"$exists": True}
                for field in must_have_local_fields
            }
        }
        pipeline.append(match_expr)
    if local_filter_dict:
        match_expr = {"$match": local_filter_dict}
        pipeline.append(match_expr)

    # early project with only necessary local fields to reduce data transfer during lookup
    local_project_expr = {
        "$project": to_mongo_projection(include_fields=[local_id_field, *local_fields])
    }
    pipeline.append(local_project_expr)

    as_name = as_name or f"{foreign_collection}_agg"
    lookup_expr = {
        "$lookup": {
            "from": foreign_collection,
            "localField": local_id_field,
            "foreignField": foreign_id_field,
            "as": as_name,
        }
    }
    pipeline.append(lookup_expr)

    if must_in_foreign_ids:
        match_expr = {
            "$match": {f"{as_name}.{foreign_id_field}": {"$in": must_in_foreign_ids}}
        }
        pipeline.append(match_expr)
    if must_have_foreign_fields:
        if isinstance(must_have_foreign_fields, str):
            must_have_foreign_fields = [must_have_foreign_fields]
        match_expr = {
            "$match": {
                f"{as_name}.{field}": {"$exists": True}
                for field in must_have_foreign_fields
            }
        }
        pipeline.append(match_expr)
    if foreign_filter_dict:
        match_expr = {"$match": foreign_filter_dict}
        pipeline.append(match_expr)

    local_fields_project = {field: 1 for field in local_fields}
    foreign_fields_project = {f"{as_name}.{field}": 1 for field in foreign_fields}
    unwind_project_expr = [
        {"$unwind": f"${as_name}"},
        {"$project": {**local_fields_project, **foreign_fields_project}},
    ]
    pipeline.extend(unwind_project_expr)

    return pipeline
