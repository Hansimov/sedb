import importlib
import sys


def _clear_sedb_modules():
    for name in list(sys.modules):
        if name == "sedb" or name.startswith("sedb."):
            sys.modules.pop(name, None)


def test_import_sedb_does_not_eagerly_import_optional_backends():
    _clear_sedb_modules()

    sedb = importlib.import_module("sedb")

    assert sedb is not None
    assert "sedb.faiss" not in sys.modules
    assert "sedb.milvus" not in sys.modules
    assert "sedb.qdrant" not in sys.modules


def test_accessing_elastic_operator_does_not_import_faiss():
    _clear_sedb_modules()

    from sedb import ElasticOperator

    assert ElasticOperator is not None
    assert "sedb.elastic" in sys.modules
    assert "sedb.faiss" not in sys.modules
