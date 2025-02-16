from collections.abc import Generator
from typing import Union

from .milvus import MilvusOperator


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

    def filter_ids_batch_generator(
        self,
        ids_batch_generator: Generator[list[str], None, None],
        id_field: str,
        expr: str = None,
        output_fields: list[str] = None,
        batch_size: int = 1000,
    ) -> Generator[list[dict], None, None]:
        filter_params = {
            "id_field": id_field,
            "expr": expr,
            "output_fields": output_fields,
        }
        res_docs = []
        for idx, ids_batch in enumerate(ids_batch_generator):
            res_docs.extend(self.filter_ids(ids_batch, **filter_params))
            if len(res_docs) >= batch_size:
                yield res_docs
                res_docs = []
        if res_docs:
            yield res_docs
