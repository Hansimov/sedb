from tclogger import logger, dict_to_str
from typing import Union

from .milvus import MilvusOperator
from .elastic import ElasticOperator
from .elastic_filter import to_elastic_filter


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
        return result["hits"]["hits"]
