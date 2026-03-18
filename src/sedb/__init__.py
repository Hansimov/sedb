from __future__ import annotations

from importlib import import_module


_EXPORTS: dict[str, tuple[str, str]] = {
    "FilterIndexType": ("sedb.mongo_types", "FilterIndexType"),
    "FilterOpType": ("sedb.mongo_types", "FilterOpType"),
    "FilterRangeType": ("sedb.mongo_types", "FilterRangeType"),
    "SortOrderType": ("sedb.mongo_types", "SortOrderType"),
    "MongoConfigsType": ("sedb.mongo_types", "MongoConfigsType"),
    "MongoCursorParamsType": ("sedb.mongo_types", "MongoCursorParamsType"),
    "MongoExtendParamsType": ("sedb.mongo_types", "MongoExtendParamsType"),
    "MongoCountParamsType": ("sedb.mongo_types", "MongoCountParamsType"),
    "MongoFilterParamsType": ("sedb.mongo_types", "MongoFilterParamsType"),
    "MongoOperator": ("sedb.mongo", "MongoOperator"),
    "ElasticOperator": ("sedb.elastic", "ElasticOperator"),
    "ElasticConfigsType": ("sedb.elastic", "ElasticConfigsType"),
    "to_elastic_filter": ("sedb.elastic_filter", "to_elastic_filter"),
    "range_to_mongo_filter_and_sort_info": (
        "sedb.mongo_filter",
        "range_to_mongo_filter_and_sort_info",
    ),
    "filter_str_to_params": ("sedb.mongo_filter", "filter_str_to_params"),
    "filter_params_to_mongo_filter": (
        "sedb.mongo_filter",
        "filter_params_to_mongo_filter",
    ),
    "filters_str_to_mongo_filter": (
        "sedb.mongo_filter",
        "filters_str_to_mongo_filter",
    ),
    "extract_count_params_from_cursor_params": (
        "sedb.mongo_filter",
        "extract_count_params_from_cursor_params",
    ),
    "extract_filter_params_from_cursor_params": (
        "sedb.mongo_filter",
        "extract_filter_params_from_cursor_params",
    ),
    "to_mongo_projection": ("sedb.mongo_pipeline", "to_mongo_projection"),
    "to_mongo_pipeline": ("sedb.mongo_pipeline", "to_mongo_pipeline"),
    "MongoDocsGenerator": ("sedb.mongo_generator", "MongoDocsGenerator"),
    "MongoDocsGeneratorArgParser": (
        "sedb.mongo_generator",
        "MongoDocsGeneratorArgParser",
    ),
    "cli_args_to_mongo_configs": (
        "sedb.mongo_generator",
        "cli_args_to_mongo_configs",
    ),
    "cli_args_to_mongo_extend_params": (
        "sedb.mongo_generator",
        "cli_args_to_mongo_extend_params",
    ),
    "RedisOperator": ("sedb.redis", "RedisOperator"),
    "RedisConfigsType": ("sedb.redis", "RedisConfigsType"),
    "RocksOperator": ("sedb.rocks", "RocksOperator"),
    "RocksConfigsType": ("sedb.rocks", "RocksConfigsType"),
    "ACCESS_READ_WRITE": ("sedb.rocks", "ACCESS_READ_WRITE"),
    "ACCESS_READ_ONLY": ("sedb.rocks", "ACCESS_READ_ONLY"),
    "ACCESS_SECONDARY": ("sedb.rocks", "ACCESS_SECONDARY"),
    "FaissOperator": ("sedb.faiss", "FaissOperator"),
    "FaissConfigsType": ("sedb.faiss", "FaissConfigsType"),
    "FaissClient": ("sedb.faiss_server", "FaissClient"),
    "FaissServer": ("sedb.faiss_server", "FaissServer"),
    "FAISS_PORT": ("sedb.faiss_server", "FAISS_PORT"),
    "MilvusOperator": ("sedb.milvus", "MilvusOperator"),
    "MilvusConfigsType": ("sedb.milvus", "MilvusConfigsType"),
    "QdrantOperator": ("sedb.qdrant", "QdrantOperator"),
    "QdrantConfigsType": ("sedb.qdrant", "QdrantConfigsType"),
    "MongoBridger": ("sedb.bridger", "MongoBridger"),
    "MilvusBridger": ("sedb.bridger", "MilvusBridger"),
    "ElasticBridger": ("sedb.bridger", "ElasticBridger"),
    "RocksBridger": ("sedb.bridger", "RocksBridger"),
    "LLMConfigsType": ("sedb.llm", "LLMConfigsType"),
    "LLMClient": ("sedb.llm", "LLMClient"),
    "LLMClientByConfig": ("sedb.llm", "LLMClientByConfig"),
    "EmbedConfigsType": ("sedb.embed", "EmbedConfigsType"),
    "EmbedClient": ("sedb.embed", "EmbedClient"),
    "EmbedClientByConfig": ("sedb.embed", "EmbedClientByConfig"),
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str):
    if name not in _EXPORTS:
        raise AttributeError(name)
    module_name, attr_name = _EXPORTS[name]
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
