import json

import httpx


class ProviderError(Exception):
    def __init__(self, message, retryable=True, rejected=False):
        super().__init__(message)
        self.retryable = retryable
        self.rejected = rejected


def object_value(value):
    if not isinstance(value, dict):
        raise ValueError("Expected object")
    return value


def token_count(value):
    # Счётчики хранятся в PostgreSQL integer; повреждённый usage
    # отклоняется в адаптере до попытки записи в ledger.
    if type(value) is not int or not 0 <= value <= 2147483647:
        raise ValueError("Expected token count within PostgreSQL integer range")
    return value


def string_value(value):
    if not isinstance(value, str):
        raise ValueError("Expected string")
    value.encode("utf-8")
    return value


async def sse_events(response):
    data = []
    size = 0
    event_size = 0
    async for line in response.aiter_lines():
        size += len(line.encode())
        event_size += len(line.encode())
        if size > 2000000 or event_size > 65536:
            raise ProviderError("Upstream stream exceeds configured size")
        if not line:
            if data:
                yield "\n".join(data)
            data = []
            event_size = 0
        elif line.startswith("data:"):
            data.append(line[5:].lstrip())
    if data:
        yield "\n".join(data)


async def stream(client, provider, body):
    headers = {"Accept": "text/event-stream"}
    if provider.protocol == "openai":
        path = "/v1/chat/completions"
        headers["Authorization"] = "Bearer " + provider.api_key.get_secret_value()
        payload = {
            "model": provider.model,
            "messages": body["messages"],
            "max_completion_tokens": body["max_tokens"],
            "stream": True,
            "stream_options": {"include_usage": True},
        }
    elif provider.protocol == "anthropic":
        path = "/v1/messages"
        headers.update(
            {"x-api-key": provider.api_key.get_secret_value(), "anthropic-version": "2023-06-01"}
        )
        messages = [m for m in body["messages"] if m["role"] != "system"]
        payload = {
            "model": provider.model,
            "messages": messages,
            "max_tokens": body["max_tokens"],
            "stream": True,
        }
        system = "\n".join(m["content"] for m in body["messages"] if m["role"] == "system")
        if system:
            payload["system"] = system
    else:
        raise ProviderError("Unsupported provider protocol", retryable=False, rejected=True)
    finished = False
    usage = None
    final_usage = False
    async with client.stream(
        "POST", provider.base_url.rstrip("/") + path, json=payload, headers=headers
    ) as response:
        if response.status_code != 200:
            code = response.status_code
            raise ProviderError(
                f"upstream_http_{code}",
                retryable=code == 429 or code >= 500,
                rejected=400 <= code < 500,
            )
        if "text/event-stream" not in response.headers.get("content-type", ""):
            raise ProviderError("Expected upstream SSE")
        async for raw in sse_events(response):
            if raw == "[DONE]" and provider.protocol == "openai":
                finished = True
                break
            try:
                event = object_value(json.loads(raw))
                if event.get("error") or event.get("type") == "error":
                    raise ProviderError("upstream_stream_error")
                if provider.protocol == "openai":
                    if event.get("usage") is not None:
                        u = object_value(event["usage"])
                        usage = (
                            token_count(u["prompt_tokens"]),
                            token_count(u["completion_tokens"]),
                        )
                        final_usage = True
                    choices = event.get("choices", [])
                    if not isinstance(choices, list):
                        raise ValueError("Expected choices array")
                    texts = []
                    for choice in choices:
                        delta = object_value(object_value(choice).get("delta", {}))
                        content = delta.get("content")
                        if content is not None:
                            content = string_value(content)
                            if content:
                                texts.append(content)
                    # Проверяем всё событие до первого yield: повреждённый choice
                    # не должен частично выдать текст и закрыть возможность fallback.
                    for content in texts:
                        yield {"type": "delta", "text": content}
                else:
                    kind = string_value(event.get("type"))
                    if kind == "message_start":
                        u = object_value(object_value(event["message"])["usage"])
                        usage = (
                            token_count(
                                token_count(u["input_tokens"])
                                + token_count(u.get("cache_creation_input_tokens", 0))
                                + token_count(u.get("cache_read_input_tokens", 0))
                            ),
                            token_count(u["output_tokens"]),
                        )
                    elif kind == "content_block_delta":
                        delta = object_value(event["delta"])
                        if string_value(delta.get("type")) == "text_delta":
                            content = string_value(delta["text"])
                            if content:
                                yield {"type": "delta", "text": content}
                    elif kind == "message_delta" and "usage" in event:
                        # Anthropic передаёт накопленный output_tokens; складывать такие события нельзя.
                        if usage is None:
                            raise ValueError("Missing message_start usage")
                        u = object_value(event["usage"])
                        usage = (usage[0], token_count(u["output_tokens"]))
                        final_usage = True
                    elif kind == "message_stop":
                        finished = True
                        break
            except (KeyError, ValueError, TypeError) as error:
                raise ProviderError("Malformed upstream event") from error
    if not finished:
        raise ProviderError("Upstream closed without terminal event")
    if not final_usage or usage is None or min(usage) < 0:
        raise ProviderError("Final usage missing")
    yield {"type": "usage", "input_tokens": usage[0], "output_tokens": usage[1]}


def make_client(timeout):
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout, connect=2, pool=2),
        follow_redirects=False,
        trust_env=False,
        limits=httpx.Limits(max_connections=32, max_keepalive_connections=16),
    )
