import argparse
import requests
import uvicorn

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from tclogger import PathType
from typing import Optional

from .faiss import FaissOperator, EidType

FAISS_PORT = 28415

NoneField = Field(default=None, examples=[None])
EmbsType = list[float]
OptEmbsType = Optional[EmbsType]


class GetEmbByEidRequest(BaseModel):
    eid: EidType


GetEmbByEidResponse = OptEmbsType


class TopRequest(BaseModel):
    emb: OptEmbsType = NoneField
    eid: Optional[EidType] = NoneField
    topk: int = Field(default=10, ge=1)
    efSearch: Optional[int] = NoneField
    return_emb: bool = False


TotalCountResponse = int


class TopResultItem(BaseModel):
    eid: EidType
    emb: OptEmbsType = NoneField
    similarity: float


TopResponse = list[TopResultItem]


class FaissServer:
    def __init__(
        self,
        db_path: PathType,
        host: str = "0.0.0.0",
        port: int = FAISS_PORT,
    ):
        self.db_path = db_path
        self.host = host
        self.port = port
        self.init_faiss()
        self.init_app()

    def init_faiss(self):
        self.faiss = FaissOperator(db_path=self.db_path)
        self.faiss.load_db()

    def init_app(self):
        self.app = FastAPI(
            title="Faiss Server",
            version="0.1.0",
            docs_url="/",
            swagger_ui_parameters={"defaultModelsExpandDepth": -1},
        )
        self.setup_routes()

    async def total_count(self) -> TotalCountResponse:
        """Get total count of embeddings in the index."""
        count = self.faiss.total_count()
        return count

    async def get_emb_by_eid(self, r: GetEmbByEidRequest) -> GetEmbByEidResponse:
        """Get embedding by external id (eid)."""
        emb = self.faiss.get_emb_by_eid(r.eid)
        if emb is None:
            return None
        return emb.tolist()

    async def top(self, r: TopRequest) -> TopResponse:
        """Search for top-k most similar embeddings."""
        if r.emb is None and r.eid is None:
            raise HTTPException(
                status_code=400,
                detail="Either 'emb' or 'eid' must be provided",
            )
        try:
            results = self.faiss.top(
                emb=r.emb,
                eid=r.eid,
                topk=r.topk,
                efSearch=r.efSearch,
                return_emb=r.return_emb,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        items = []
        for eid, emb, sim in results:
            emb_list = emb.tolist() if emb is not None else None
            items.append(TopResultItem(eid=eid, emb=emb_list, similarity=sim))
        return items

    def setup_routes(self):
        self.app.get(
            "/total_count",
            response_model=TotalCountResponse,
            summary="Get total count",
        )(self.total_count)

        self.app.post(
            "/get_emb_by_eid",
            response_model=GetEmbByEidResponse,
            summary="Get embedding by eid",
        )(self.get_emb_by_eid)

        self.app.post(
            "/top",
            response_model=TopResponse,
            summary="Top-k similarity search",
        )(self.top)

    def run(self):
        uvicorn.run(self.app, host=self.host, port=self.port)


class FaissClient:
    def __init__(self, host: str = "localhost", port: int = FAISS_PORT):
        self.host = host
        self.port = port
        self.endpoint = f"http://{host}:{port}"

    def total_count(self) -> TotalCountResponse:
        """Get total count of embeddings in the index."""
        resp = requests.get(f"{self.endpoint}/total_count")
        resp.raise_for_status()
        return resp.json()

    def get_emb_by_eid(self, eid: EidType) -> GetEmbByEidResponse:
        """Get embedding by external id (eid)."""
        resp = requests.post(
            f"{self.endpoint}/get_emb_by_eid",
            json={
                "eid": eid,
            },
        )
        resp.raise_for_status()
        return resp.json()

    def top(
        self,
        emb: OptEmbsType = None,
        eid: Optional[EidType] = None,
        topk: int = 10,
        efSearch: Optional[int] = None,
        return_emb: bool = False,
    ) -> TopResponse:
        """Search for top-k most similar embeddings."""
        resp = requests.post(
            f"{self.endpoint}/top",
            json={
                "emb": emb,
                "eid": eid,
                "topk": topk,
                "efSearch": efSearch,
                "return_emb": return_emb,
            },
        )
        resp.raise_for_status()
        return resp.json()


class FaissServerArgParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_argument("-d", "--db-path", type=str, required=True)
        self.add_argument("-H", "--host", type=str, default="0.0.0.0")
        self.add_argument("-P", "--port", type=int, default=FAISS_PORT)


def main():
    args = FaissServerArgParser().parse_args()
    server = FaissServer(
        db_path=args.db_path,
        host=args.host,
        port=args.port,
    )
    server.run()


if __name__ == "__main__":
    main()

    # run server
    # python -m sedb.faiss_server -d /media/data/tembed/qwen3_06b.faiss
