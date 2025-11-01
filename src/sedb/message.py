from tclogger import logger, logstr, brk, get_now_str


class ConnectMessager:
    def __init__(
        self,
        msg: str = None,
        src_msg: str = None,
        dst_msg: str = None,
        cls: type = None,
        opr: type = None,
        dbt: str = None,
        indent: int = 0,
        verbose: bool = True,
    ):
        """
        - msg: direct message to log
        - src_msg: source message (e.g., class name)
        - dst_msg: destination  message (e.g., database name)
        - cls: class of instance using MongoOperator/ElasticOperator/RedisOperator
        - opr: database operator object (e.g., MongoOperator, ElasticOperator, RedisOperator)
        - dbt: database type (e.g., "mongo", "elastic", "redis")
        """
        self.msg = msg
        self.src_msg = src_msg
        self.dst_msg = dst_msg
        self.cls = cls
        self.dbt = dbt
        self.dbp = opr
        self.indent = indent
        self.verbose = verbose

    @property
    def vparams(self):
        return {"indent": self.indent, "verbose": self.verbose}

    def log_now(self):
        logger.file(f"  * {get_now_str()}", **self.vparams)

    def log_msg(self):
        if not self.msg:
            if self.src_msg:
                src_msg_str = f"{logstr.mesg(self.src_msg)}"
            elif self.cls:
                src_msg_str = f"{logstr.mesg(self.cls.__name__)}"
            else:
                src_msg_str = ""

            if self.dst_msg:
                dst_msg_str = f"{logstr.okay(self.dst_msg)}"
            elif self.dbt:
                dst_msg_str = f"{logstr.okay(brk(self.dbt))}"
            else:
                dst_msg_str = ""

            msg_str = f"  * {src_msg_str} -> {dst_msg_str}"
        else:
            msg_str = f"  * {self.msg}"

        logger.mesg(msg_str, **self.vparams)

    def log_endpoint(self):
        logger.note(
            f"> Connecting to: {logstr.mesg(brk(self.dbp.endpoint))}",
            **self.vparams,
        )

    def log_dbname(self):
        logger.file(
            f"  * database: {logstr.success(self.dbp.dbname)}",
            **self.vparams,
        )
