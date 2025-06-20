import asyncio
import ast
import json
import re
import requests

from tclogger import logger, logstr, dict_to_str, dt_to_str, Runtimer
from typing import Literal, TypedDict

API_FORMAT_TYPE = Literal["openai", "ollama"]


class LLMConfigsType(TypedDict):
    endpoint: str
    api_key: str
    model: str
    api_format: API_FORMAT_TYPE = "openai"
    stream: bool = None
    init_messages: list = []
    enable_thinking: bool = None
    delta_func: callable = None
    terminate_event: asyncio.Event = None
    verbose_user: bool = True
    verbose_assistant: bool = True
    verbose_think: bool = True
    verbose_content: bool = True
    verbose_usage: bool = True
    verbose_finish: bool = True
    verbose: bool = False


class LLMClient:
    def __init__(
        self,
        endpoint: str,
        api_key: str,
        api_format: API_FORMAT_TYPE = "openai",
        model: str = None,
        stream: bool = None,
        init_messages: list = [],
        enable_thinking: bool = None,
        delta_func: callable = None,
        terminate_event: asyncio.Event = None,
        verbose_user: bool = True,
        verbose_assistant: bool = True,
        verbose_content: bool = True,
        verbose_think: bool = True,
        verbose_usage: bool = True,
        verbose_finish: bool = True,
        verbose: bool = False,
    ):
        self.endpoint = endpoint
        self.api_key = api_key
        self.api_format = api_format
        self.model = model
        self.stream = stream
        self.init_messages = init_messages
        self.enable_thinking = enable_thinking
        self.delta_func = delta_func
        self.terminate_event = terminate_event

        self.verbose_user = verbose_user
        self.verbose_assistant = verbose_assistant
        self.verbose_content = verbose_content
        self.verbose_think = verbose_think
        self.verbose_usage = verbose_usage
        self.verbose_finish = verbose_finish
        self.verbose = verbose

        self.is_thinking = False

    def get_model_str(self, model: str = None, default: str = "gpt-3.5-turbo") -> str:
        if model is None and self.model is None:
            model = default
        elif model is None:
            model = self.model
        else:
            model = model
        return model

    def get_stream_bool(self, stream: bool = None, default: bool = True) -> bool:
        if stream is None and self.stream is None:
            stream = default
        elif stream is None:
            stream = self.stream
        else:
            stream = stream
        return stream

    def set_enable_thinking(self, enable_thinking: bool = None) -> bool:
        if enable_thinking is None:
            self.enable_thinking = self.enable_thinking
        else:
            self.enable_thinking = enable_thinking
        return self.enable_thinking

    def set_think_status(self, tag: str = "<think>"):
        if self.enable_thinking is not False and not self.is_thinking:
            self.is_thinking = True
            logger.mesg(tag, end="", verbose=self.verbose_think)

    def reset_think_status(self, tag: str = "</think>"):
        if self.enable_thinking is not False and self.is_thinking:
            self.is_thinking = False
            logger.mesg(tag, end="", verbose=self.verbose_think)

    def create_response(
        self,
        messages: list,
        model: str = None,
        enable_thinking: bool = None,
        temperature: float = None,
        seed: int = None,
        stream: bool = None,
    ):
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
        }
        if self.api_key:
            headers["authorization"] = f"Bearer {self.api_key}"
        model = self.get_model_str(model)
        enable_thinking = self.set_enable_thinking(enable_thinking)
        stream = self.get_stream_bool(stream)
        payload = {
            "model": model,
            "messages": self.init_messages + messages,
            "stream": stream,
        }
        options = {}
        if temperature is not None:
            options["temperature"] = temperature
        if seed is not None:
            options["seed"] = seed
        if self.api_format == "ollama":
            payload["options"] = options
        else:
            payload.update(options)
        if enable_thinking is not None:
            payload["chat_template_kwargs"] = {"enable_thinking": enable_thinking}
        response = requests.post(
            self.endpoint, headers=headers, json=payload, stream=stream
        )
        return response

    def exec_delta_func(self, role: str, content: str):
        if self.delta_func:
            delta_func_args = {"role": role, "content": content}
            if asyncio.iscoroutinefunction(self.delta_func):
                asyncio.run(self.delta_func(**delta_func_args))
            else:
                self.delta_func(**delta_func_args)

    def parse_stream_response(self, response: requests.Response) -> tuple[str, dict]:
        response_content = ""
        usage = None
        for line in response.iter_lines():
            if self.terminate_event and self.terminate_event.is_set():
                break

            line = line.decode("utf-8")
            remove_patterns = [r"^\s*data:\s*", r"^\s*\[DONE\]\s*"]
            for pattern in remove_patterns:
                line = re.sub(pattern, "", line).strip()

            if line:
                try:
                    line_data = json.loads(line)
                except Exception as e:
                    try:
                        line_data = ast.literal_eval(line)
                    except:
                        logger.warn(f"× Error: {line}")
                        raise e

                if self.api_format == "ollama":
                    # https://github.com/ollama/ollama/blob/main/docs/api.md#response-9
                    delta_data = line_data["message"]
                    finish_reason = "stop" if line_data["done"] else None
                else:
                    # https://platform.openai.com/docs/api-reference/chat/streaming
                    delta_data = line_data["choices"][0]["delta"]
                    finish_reason = line_data["choices"][0].get("finish_reason", None)
                if "role" in delta_data:
                    role = delta_data["role"]
                if "reasoning_content" in delta_data:
                    self.set_think_status()
                    delta_content = delta_data["reasoning_content"]
                    response_content += delta_content
                    logger.mesg(delta_content, end="", verbose=self.verbose_content)
                if "content" in delta_data:
                    self.reset_think_status()
                    delta_content = delta_data["content"]
                    response_content += delta_content
                    logger.mesg(delta_content, end="", verbose=self.verbose_content)
                    self.exec_delta_func(role, delta_content)
                if finish_reason == "stop":
                    if line_data.get("usage", {}):
                        usage = line_data["usage"]
                        logger.file(
                            "\n" + dict_to_str(usage), verbose=self.verbose_usage
                        )
                    logger.success("\n[Finished]", end="", verbose=self.verbose_finish)
                    self.exec_delta_func("stop", "")

        return response_content, usage

    def parse_json_response(self, response: requests.Response) -> tuple[str, dict]:
        response_content = ""
        usage = None
        try:
            response_data = response.json()
            response_content = ""
            if self.api_format == "ollama":
                message = response_data["message"]
            else:
                message = response_data["choices"][0]["message"]
            reasoning_content = message.get("reasoning_content", "")
            content = message.get("content", "")
            if reasoning_content and content:
                response_content = f"<think>{reasoning_content}</think>" + content
            elif reasoning_content:
                response_content = reasoning_content
            else:
                response_content = content
            if "usage" in response_data:
                usage = response_data["usage"]
                if usage and self.verbose_usage:
                    logger.file("\n" + dict_to_str(usage))
            if self.verbose_content:
                logger.mesg(response_content)
            if self.verbose_finish:
                logger.success("[Finished]", end="")
        except Exception as e:
            logger.warn(f"× Error: {response.text}")
        return response_content, usage

    def chat(
        self,
        messages: list,
        model: str = None,
        enable_thinking: bool = None,
        temperature: float = None,
        seed: int = None,
        stream: bool = None,
        verbose: bool = None,
    ) -> str:
        timer = Runtimer(verbose=False)
        timer.start_time()
        model = self.get_model_str(model)
        stream = self.get_stream_bool(stream)

        verbose = verbose if verbose is not None else self.verbose
        logger.enter_quiet(not verbose)

        if self.verbose_user:
            user_prompt = messages[-1]["content"]
            logger.note(f"USER: {user_prompt}")

        response = self.create_response(
            messages=messages,
            model=model,
            enable_thinking=enable_thinking,
            temperature=temperature,
            seed=seed,
            stream=stream,
        )

        if self.verbose_assistant:
            logger.mesg("ASSISTANT: ", end="")

        if stream:
            response_content, usage = self.parse_stream_response(response)
        else:
            response_content, usage = self.parse_json_response(response)
        timer.end_time()
        if self.verbose_finish:
            elapsed_time = dt_to_str(
                timer.elapsed_time(), precision=1, str_format="unit"
            )
            model_name_str = "[" + model.split("/")[-1] + "]"
            logger.note(f" ({elapsed_time}) {logstr.file(model_name_str)}")
        else:
            logger.note("", verbose=self.verbose_content)

        logger.exit_quiet(not verbose)
        return response_content


class LLMClientByConfig(LLMClient):
    def __init__(self, configs: LLMConfigsType):
        super().__init__(**configs)
