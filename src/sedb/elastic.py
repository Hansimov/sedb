from elasticsearch import Elasticsearch
from tclogger import logger, logstr
from typing import TypedDict


class ElasticConfigsType(TypedDict):
    host: str
    port: int
    ca_certs: str
    api_key: str


class ElasticOperator:
    def __init__(self, configs: ElasticConfigsType, verbose: bool = True):
        self.configs = configs
        self.init_configs()
        self.verbose = verbose

    def init_configs(self):
        self.host = self.configs["host"]
        self.port = self.configs["port"]
        self.ca_certs = self.configs["ca_certs"]
        self.api_key = self.configs["api_key"]
        self.endpoint = f"https://{self.host}:{self.port}"

    def connect(self):
        """Connect to self-managed cluster with API Key authentication
        * https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html#auth-apikey

        How to create API Key:
        - Go to Kibana: http://<hostname>:5601/app/management/security/api_keys
        - Create API Key, which would generated a json with keys "name", "api_key" and "encoded"
        - Use "encoded" value for the `api_key` param in Elasticsearch class below

        Connect to self-managed cluster with HTTP Bearer authentication
        * https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html#auth-bearer
        """
        if self.verbose:
            logger.note(
                f"> Connecting to Elasticsearch: "
                f"{logstr.mesg('['+self.endpoint+']')}"
            )
        try:
            self.client = Elasticsearch(
                hosts=self.endpoint,
                ca_certs=self.ca_certs,
                api_key=self.api_key,
                # basic_auth=(self.username, self.password),
            )
            if self.verbose:
                logger.success(f"+ Connected:")
                logger.mesg(self.client.info())
        except Exception as e:
            raise e
