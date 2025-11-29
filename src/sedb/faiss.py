import faiss
import numpy as np
import pickle

from tclogger import PathType, TCLogger
from pathlib import Path
from typing import Optional, TypedDict, Union

logger = TCLogger()


class FaissConfigsType(TypedDict):
    db_path: str
    dim: Optional[int]
    M: Optional[int]
    efConstruction: Optional[int]
    efSearch: Optional[int]


EmbType = Union[list[float], np.ndarray]
EidType = Union[str, int]
SimType = Union[float, int]


def norm_embs(embs: np.ndarray) -> np.ndarray:
    """Normalize embeddings for cosine similarity."""
    embs = np.array(embs, dtype=np.float32)
    if embs.ndim == 1:
        embs = embs.reshape(1, -1)
    faiss.normalize_L2(embs)
    return embs


def unify_eids_embs(
    eids: list[EidType], embs: EmbType
) -> tuple[list[EidType], np.ndarray]:
    """Unify and pick valid pairs of eids and embeddings.
    Return: (valid_eids, embs_arr)
    """
    pairs = [(eid, emb) for eid, emb in zip(eids, embs) if eid and emb is not None]
    if not pairs:
        return [], np.array([], dtype="float32")
    valid_eids = [eid for eid, _ in pairs]
    embs_arr = np.array([emb for _, emb in pairs], dtype="float32")
    return valid_eids, embs_arr


class FaissOperator:
    def __init__(
        self,
        db_path: PathType,
        dim: int = None,
        M: int = 32,
        efConstruction: int = 40,
        efSearch: int = 64,
    ):
        """
        Params:
        - `db_path`: faiss index file path

        `init_db()` requires:
            - `dim`: embedding dimension
            - `M`: num of connections per layer
                * 32 is good for 10M scale
            - `efConstruction`: search depth during construction
                * higher = better quality but slower
            - `efSearch`: search depth during query
                * higher = better recall but slower

        Vars:
        - `iid`: internal id, used by faiss, must be int
        - `eid`: external id, used by user, can be str or int
        - `nid`: next iid to assign, used in `add_embs()`
        """
        self.db_path = Path(db_path)
        self.map_path = Path(str(self.db_path) + ".map.pkl")
        self.dim = dim
        self.M = M
        self.efConstruction = efConstruction
        self.efSearch = efSearch
        self.eid_to_iid: dict[str, int] = {}
        self.iid_to_eid: dict[int, str] = {}
        self.nid: int = 0
        self.db: faiss.IndexIDMap = None

    def _log_params(self):
        logger.mesg(
            f"  * dim={self.dim}, M={self.M}, "
            f"efConstruction={self.efConstruction}, efSearch={self.efSearch}"
        )

    def _log_db_info(self):
        logger.okay(f"  * db_path: {self.db_path}")
        logger.file(f"  * {self.total_count()} rows")

    def _log_mappings(self):
        logger.okay(f"  * map_path: {self.map_path}")
        logger.file(f"  * {len(self.iid_to_eid)} mappings")

    def init_db(self):
        """Create new faiss index"""
        if self.dim is None:
            raise ValueError("'dim' must be provided for init_db")
        logger.note(f"> Init Faiss HNSW index with IDMap ...")
        # IndexHNSWFlat: HNSW graph + flat storage, supports reconstruct()
        # METRIC_INNER_PRODUCT: inner product on normalized vectors = cosine similarity
        hnsw_index = faiss.IndexHNSWFlat(self.dim, self.M, faiss.METRIC_INNER_PRODUCT)
        hnsw_index.hnsw.efConstruction = self.efConstruction
        hnsw_index.hnsw.efSearch = self.efSearch
        # IndexIDMap: wrapper that allows custom int64 IDs
        self.db = faiss.IndexIDMap(hnsw_index)
        self._log_params()

    def _load_params(self):
        """Load index params from loaded index"""
        hnsw_index = faiss.downcast_index(self.db.index)
        self.dim = hnsw_index.d
        # M is not directly accessible; level 0 has 2*M neighbors, level 1+ has M
        self.M = hnsw_index.hnsw.nb_neighbors(1)
        self.efConstruction = hnsw_index.hnsw.efConstruction
        self.efSearch = hnsw_index.hnsw.efSearch
        self._log_params()

    def _load_mappings(self):
        """Load iid-eid mappings"""
        if not self.map_path.exists():
            logger.warn(f"× Mappings file not found: {self.map_path}")
            return
        logger.note(f"> Load iid-eid mappings:")
        with open(self.map_path, "rb") as f:
            self.iid_to_eid = pickle.load(f)
        self.eid_to_iid = {eid: idx for idx, eid in self.iid_to_eid.items()}
        self.nid = max(self.iid_to_eid.keys()) + 1 if self.iid_to_eid else 0
        self._log_mappings()

    def load_db(self):
        """Load existed faiss index"""
        if not self.db_path.exists():
            raise FileNotFoundError(f"Index file not found: {self.db_path}")
        logger.note(f"> Load Faiss index:")
        self.db = faiss.read_index(str(self.db_path))
        self._log_db_info()
        self._load_params()
        self._load_mappings()

    def add_embs(self, eids: list[str], embs: np.ndarray):
        """Add embeddings to index. Skip existed eids, as index update is not allowed in hnsw."""
        if len(eids) == 0 or len(embs) == 0:
            return
        xeids, xembs = unify_eids_embs(eids, embs)
        new_iids: list[int] = []
        new_embs_idxs: list[int] = []
        for i, eid in enumerate(xeids):
            if eid in self.eid_to_iid:
                continue
            self.eid_to_iid[eid] = self.nid
            self.iid_to_eid[self.nid] = eid
            new_iids.append(self.nid)
            new_embs_idxs.append(i)
            self.nid += 1
        if new_iids:
            new_iids_arr = np.array(new_iids, dtype=np.int64)
            new_embs = xembs[new_embs_idxs]
            new_embs_arr = norm_embs(new_embs)
            self.db.add_with_ids(new_embs_arr, new_iids_arr)

    def _save_index(self):
        if self.db is None:
            logger.warn("× No index to save")
            return
        logger.note(f"> Save index:")
        faiss.write_index(self.db, str(self.db_path))
        self._log_db_info()

    def _save_mappings(self):
        logger.note(f"> Save iid-eid mappings:")
        with open(self.map_path, "wb") as f:
            pickle.dump(self.iid_to_eid, f, protocol=pickle.HIGHEST_PROTOCOL)
        self._log_mappings()

    def save(self):
        self._save_index()
        self._save_mappings()

    def total_count(self) -> int:
        if self.db is None:
            return 0
        return self.db.ntotal

    def get_emb_by_eid(self, eid: EidType) -> Optional[np.ndarray]:
        """Get embedding by eid"""
        if eid not in self.eid_to_iid:
            return None
        iid = self.eid_to_iid[eid]
        hnsw_index = faiss.downcast_index(self.db.index)
        emb = hnsw_index.reconstruct(iid)
        return emb

    def _set_search_params(self, efSearch: int = None):
        """Must call this before using `top()`"""
        self.efSearch = efSearch or self.efSearch
        hnsw_index = faiss.downcast_index(self.db.index)
        hnsw_index.hnsw.efSearch = self.efSearch

    def top(
        self,
        emb: EmbType = None,
        eid: EidType = None,
        topk: int = 10,
        efSearch: int = None,
        return_emb: bool = False,
    ) -> list[tuple[EidType, Optional[EmbType], SimType]]:
        """
        Search for the top-k most similar items using cosine similarity.

        Input:
        - `emb`: Query embedding vector (will be normalized)
        - `eid`: Query by exsited eid (eid) in the database
        - `efSearch`: Search depth (higher = better recall, slower speed)
        - `topk`: Number of results to return
        - `return_emb`: Whether to include embeddings in results

        Output: List of (eid, embedding, similarity) tuples.
            - Similarity is cosine similarity in range [-1, 1].
            - If `return_emb` is False, embedding will be None.
        """
        if emb is None and eid is None:
            raise ValueError("Either 'emb' or 'eid' must be provided")
        # get query embedding
        if emb is not None:
            query_emb = norm_embs(emb)
        else:
            query_emb = self.get_emb_by_eid(eid)
            if query_emb is None:
                raise ValueError(f"Eid '{eid}' not found in database")
            query_emb = query_emb.reshape(1, -1)
            # already normalized when stored, but ensure it's normalized
            faiss.normalize_L2(query_emb)
        # set search params and perform search
        self._set_search_params(efSearch=efSearch)
        similarities, iids = self.db.search(query_emb, topk)
        # build results
        results: list[tuple[EidType, Optional[EmbType], SimType]] = []
        if return_emb:
            hnsw_index = faiss.downcast_index(self.db.index)
        else:
            hnsw_index = None
        for sim, iid in zip(similarities[0], iids[0]):
            if iid == -1:  # Invalid index (fewer results than topk)
                continue
            eid = self.iid_to_eid.get(iid)
            if eid is None:
                continue
            if return_emb:
                res_emb = hnsw_index.reconstruct(int(iid))
            else:
                res_emb = None
            results.append((eid, res_emb, float(sim)))
        return results
