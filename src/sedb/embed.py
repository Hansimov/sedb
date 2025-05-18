import requests

from tclogger import logger
from typing import Literal, Union, TypedDict

API_FORMAT_TYPE = Literal["openai"]


class EmbedConfigsType(TypedDict):
    endpoint: str
    api_key: str
    model: str
    api_format: API_FORMAT_TYPE = "openai"


class EmbedClient:
    def __init__(
        self,
        endpoint: str,
        api_key: str,
        api_format: API_FORMAT_TYPE = "openai",
        model: str = None,
        verbose: bool = False,
    ):
        self.endpoint = endpoint
        self.api_key = api_key
        self.api_format = api_format
        self.model = model
        self.verbose = verbose

    def get_model_str(self, model: str = None, default: str = None) -> str:
        if model is None and self.model is None:
            model = default
        elif model is None:
            model = self.model
        else:
            model = model
        return model

    def create_response(self, input: Union[str, list[str]] = None, model: str = None):
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
        }
        if self.api_key:
            headers["authorization"] = f"Bearer {self.api_key}"
        model = self.get_model_str(model)
        payload = {"model": model, "input": input}
        response = requests.post(self.endpoint, headers=headers, json=payload)
        return response

    def parse_response(self, response: requests.Response) -> list[list[float]]:
        # https://platform.openai.com/docs/api-reference/embeddings/create?lang=curl
        try:
            resp_json = response.json()
            data = resp_json.get("data", [])
            return [item.get("embedding", []) for item in data]
        except Exception as e:
            logger.warn(f"× Error: {response.text}")
        return []

    def embed(
        self, input: Union[str, list[str]] = None, model: str = None
    ) -> list[list[float]]:
        response = self.create_response(input=input, model=model)
        return self.parse_response(response)


class EmbedClientByConfig(EmbedClient):
    def __init__(self, configs: EmbedConfigsType):
        super().__init__(**configs)
