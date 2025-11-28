import faiss
import numpy as np
import pickle

from tclogger import PathType, TCLogger
from pathlib import Path
from typing import Optional, TypedDict

logger = TCLogger()


class FaissConfigsType(TypedDict):
    db_path: str
    dim: int
    M: Optional[int]
    efConstruction: Optional[int]
    efSearch: Optional[int]


class FaissOperator:
    def __init__(
        self,
        db_path: PathType,
        dim: int,
        M: int = 32,
        efConstruction: int = 40,
        efSearch: int = 64,
    ):
        """
        - `dim`: embedding dimension
        - `M`: num of connections per layer (32 is good for 10M scale)
        - `efConstruction`: search depth during construction (higher = better quality but slower)
        - `efSearch`: search depth during query (higher = better recall but slower)
        """
        self.db_path = Path(db_path)
        self.map_path = Path(str(self.db_path) + ".map.pkl")
        self.dim = dim
        self.M = M
        self.efConstruction = efConstruction
        self.efSearch = efSearch
        self.bvid_to_id: dict[str, int] = {}
        self.id_to_bvid: dict[int, str] = {}
        self.next_id: int = 0
        self.init_db()

    def init_db(self):
        logger.note(f"> Init Faiss HNSW index with IDMap ...")
        hnsw_index = faiss.IndexHNSWFlat(self.dim, self.M)
        hnsw_index.hnsw.efConstruction = self.efConstruction
        self.db = faiss.IndexIDMap(hnsw_index)
        logger.okay(
            f"  * dim={self.dim}, M={self.M}, "
            f"efConstruction={self.efConstruction}, efSearch={self.efSearch}"
        )

    def set_search_params(self):
        hnsw_index = faiss.downcast_index(self.db.index)
        hnsw_index.hnsw.efSearch = self.efSearch

    def add_embeddings(self, bvids: list[str], embeddings: np.ndarray):
        ids = []
        for bvid in bvids:
            if bvid not in self.bvid_to_id:
                self.bvid_to_id[bvid] = self.next_id
                self.id_to_bvid[self.next_id] = bvid
                ids.append(self.next_id)
                self.next_id += 1
            else:
                ids.append(self.bvid_to_id[bvid])
        ids_array = np.array(ids, dtype=np.int64)
        self.db.add_with_ids(embeddings, ids_array)

    def save_index(self):
        if self.db is None:
            logger.warn("× No index to save")
            return
        self.set_search_params()
        logger.note(f"> Save Faiss index:")
        faiss.write_index(self.db, str(self.db_path))
        logger.okay(f"  * {self.db_path}")
        logger.mesg(f"  * {self.total_count()} rows")

    def save_mapping(self):
        logger.note(f"> Save id-bvid mappings:")
        with open(self.map_path, "wb") as f:
            pickle.dump(self.id_to_bvid, f, protocol=pickle.HIGHEST_PROTOCOL)
        logger.okay(f"  * {self.map_path}")
        logger.mesg(f"  * {len(self.id_to_bvid)} mappings")

    def save(self):
        self.save_index()
        self.save_mapping()

    def total_count(self) -> int:
        if self.db is None:
            return 0
        return self.db.ntotal
