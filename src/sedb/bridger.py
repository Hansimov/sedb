from typing import Union, Any

from .mongo import MongoOperator
from .mongo_pipeline import to_mongo_projection
from .milvus import MilvusOperator
from .elastic import ElasticOperator
from .elastic_filter import to_elastic_filter
from .rocks import RocksOperator


class MongoBridger:
    def __init__(self, mongo: MongoOperator):
        self.mongo = mongo

    def to_id_filter(self, ids: list[str], id_field: str) -> dict:
        if ids:
            return {"$match": {id_field: {"$in": ids}}}
        else:
            return None

    def filter_ids(
        self,
        collection_name: str,
        ids: list[str],
        id_field: str,
        pipeline: list[dict] = None,
        output_fields: list[str] = None,
    ) -> list[dict]:
        collect = self.mongo.db[collection_name]
        id_filter = self.to_id_filter(ids, id_field)
        if output_fields:
            projection = to_mongo_projection(include_fields=output_fields)
        else:
            projection = None
        if not pipeline:
            cursor = collect.find(filter=id_filter, projection=projection)
        else:
            full_pipeline = [id_filter, *pipeline]
            if projection:
                full_pipeline.append({"$project": projection})
            cursor = collect.aggregate(pipeline=full_pipeline)
        return list(cursor)


class MilvusBridger:
    def __init__(self, milvus: MilvusOperator):
        self.milvus = milvus

    def filter_ids(
        self,
        collection_name: str,
        ids: list[str],
        id_field: str,
        expr: str = None,
        output_fields: list[str] = None,
    ) -> list[dict]:
        expr_of_ids = self.milvus.get_expr_of_list_contain(id_field, ids)
        if expr is None:
            expr_of_res_ids = expr_of_ids
        else:
            expr_of_res_ids = f"({expr_of_ids}) AND ({expr})"

        res_docs = self.milvus.client.query(
            collection_name=collection_name,
            filter=expr_of_res_ids,
            output_fields=output_fields or [id_field],
        )
        return res_docs


class ElasticBridger:
    def __init__(self, elastic: ElasticOperator):
        self.elastic = elastic

    def filter_ids(
        self,
        index_name: str,
        ids: list[str],
        id_field: str = None,
        exprs: Union[dict, list[dict]] = None,
        output_fields: list[str] = None,
    ) -> list[dict]:
        filter_dict = to_elastic_filter(
            ids=ids, id_field=id_field, exprs=exprs, output_fields=output_fields
        )
        filter_path = "took,timed_out,hits.total,hits.hits._id"
        if id_field:
            filter_path += f",hits.hits._source.{id_field}"
        if output_fields:
            filter_path += "," + ",".join(
                [f"hits.hits._source.{field}" for field in output_fields]
            )
        search_params = {
            "index": index_name,
            "body": filter_dict,
            "filter_path": filter_path,
        }
        result = self.elastic.client.search(**search_params)
        return result["hits"].get("hits", [])


class RocksBridger:
    def __init__(self, rocks: RocksOperator):
        self.rocks = rocks

    def filter_ids(
        self, ids: list[str], return_value: bool = False
    ) -> Union[list[str], list[dict]]:
        res = []
        for id in ids:
            # NOTE: Even when key doesn't exists, `key_may_exist()`` might still return True
            # See: https://rocksdict.github.io/RocksDict/rocksdict.html#Rdict.key_may_exist
            # TODO: Improve the accuracy with `get()` when `return_value` is True
            is_exists = self.rocks.db.key_may_exist(id)
            if is_exists:
                if return_value:
                    value = self.rocks.db.get(id)
                    if value is not None:
                        res.append({"key": id, "value": value})
                else:
                    res.append(id)
        return res

    def filter_ids_with_seps(
        self,
        ids: list[str],
        return_value: bool = False,
        sep: str = ".",
        output_fields: list[str] = None,
    ) -> list[dict]:
        """
        - Inputs: `[id1, id2, id3, ...], [field1, field2, ...]`
        - Docs: `[{id.field: value}, ...]`
        - Output: `[{_id: id, field: value}]`
        """
        if output_fields is None:
            return self.filter_ids(ids, return_value=return_value)

        all_keys = [f"{id}{sep}{field}" for id in ids for field in output_fields]
        all_values = self.rocks.db.get(all_keys)

        res = []
        value_idx = 0
        for id in ids:
            doc = {"_id": id}
            is_id_exists = False
            for field in output_fields:
                value = all_values[value_idx]
                value_idx += 1
                if value is not None:
                    is_id_exists = True
                    if return_value:
                        doc[field] = value
            if is_id_exists:
                res.append(doc)

        return res

    def filter_ids_for_dict(
        self, ids: list[str], output_fields: list[str] = None
    ) -> list[dict]:
        res = []
        for id in ids:
            is_exists = self.rocks.db.key_may_exist(id)
            if is_exists:
                doc = {"_id": id}
                if output_fields:
                    value: dict = self.rocks.db.get(id)
                    if value is None:
                        continue
                    for field in output_fields:
                        if field in value:
                            doc[field] = value[field]
                res.append(doc)
        return res

    def filter_ids_for_entity(
        self, ids: list[str], output_fields: list[str] = None
    ) -> list[dict]:
        res = []
        for id in ids:
            is_exists = self.rocks.db.key_may_exist(id)
            if is_exists:
                doc = {"_id": id}
                if output_fields:
                    entity: list[tuple[Any, Any]] = self.rocks.db.get_entity(id)
                    if entity:
                        entity_dict = dict(entity)
                        for field in output_fields:
                            if field in entity_dict:
                                doc[field] = entity_dict[field]
                res.append(doc)
        return res
