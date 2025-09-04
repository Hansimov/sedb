from elasticsearch import Elasticsearch
from tclogger import logger
from typing import TypedDict

from .message import ConnectMessager


class ElasticConfigsType(TypedDict):
    host: str
    port: int
    ca_certs: str
    api_key: str


class ElasticOperator:
    def __init__(
        self,
        configs: ElasticConfigsType,
        connect_at_init: bool = True,
        connect_msg: str = None,
        connect_cls: type = None,
        verbose: bool = True,
        indent: int = 0,
    ):
        self.configs = configs
        self.connect_at_init = connect_at_init
        self.connect_msg = connect_msg
        self.indent = indent
        self.verbose = verbose
        self.init_configs()
        self.msgr = ConnectMessager(
            msg=connect_msg,
            cls=connect_cls,
            opr=self,
            dbt="elastic",
            verbose=verbose,
            indent=indent,
        )
        if self.connect_at_init:
            self.connect(connect_msg=connect_msg)

    def init_configs(self):
        self.host = self.configs["host"]
        self.port = self.configs["port"]
        self.ca_certs = self.configs["ca_certs"]
        self.api_key = self.configs["api_key"]
        self.endpoint = f"https://{self.host}:{self.port}"

    def connect(self, connect_msg: str = None):
        """Connect to self-managed cluster with API Key authentication
        * https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html#auth-apikey

        How to create API Key:
        - Go to Kibana: http://<hostname>:5601/app/management/security/api_keys
        - Create API Key, which would generated a json with keys "name", "api_key" and "encoded"
        - Use "encoded" value for the `api_key` param in Elasticsearch class below

        Connect to self-managed cluster with HTTP Bearer authentication
        * https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html#auth-bearer
        """
        self.msgr.log_endpoint()
        self.msgr.log_now()
        self.msgr.log_msg()
        try:
            self.client = Elasticsearch(
                hosts=self.endpoint,
                ca_certs=self.ca_certs,
                api_key=self.api_key,
                # basic_auth=(self.username, self.password),
            )
            if self.verbose:
                client_info = self.client.info() or {}
                client_info_str = str(dict(self.client.info() or {}))
                status_str = "✓ Connected:"
                if client_info:
                    logger.okay(status_str)
                else:
                    logger.warn(status_str)
                logger.mesg(f"  * {client_info_str}", indent=self.indent)
        except Exception as e:
            raise e
