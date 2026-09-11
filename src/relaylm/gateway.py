import asyncio
import json
from contextlib import aclosing

import anyio
import httpx

from . import circuit, ledger, providers
from .config import settings


def event(kind, **value):
    return {"type": kind, **value}


def encode(value):
    return "event: " + value["type"] + "\ndata: " + json.dumps(value, ensure_ascii=False) + "\n\n"


async def run(generation, body, client):
    completed = False
    current_attempt = None
    permit = None
    status = "cancelled"
    failure = "Client disconnected"
    try:
        yield event("start", id=str(generation["id"]))
        async with asyncio.timeout(settings.request_timeout):
            for provider in settings.providers:
                permit = await circuit.acquire(provider.name)
                if permit is None:
                    continue
                current_attempt = await ledger.start_attempt(generation, provider, body)
                content = []
                bytes_sent = 0
                usage = None
                try:
                    async with aclosing(providers.stream(client, provider, body)) as upstream:
                        async for chunk in upstream:
                            if chunk["type"] == "delta":
                                bytes_sent += len(chunk["text"].encode())
                                if bytes_sent > 65536:
                                    raise providers.ProviderError("Output exceeds 64 KiB")
                                content.append(chunk["text"])
                                yield event("delta", text=chunk["text"], provider=provider.name)
                            else:
                                usage = (chunk["input_tokens"], chunk["output_tokens"])
                    await ledger.finish_attempt(current_attempt, "completed", usage=usage)
                    await circuit.report(permit, True)
                    result = {
                        "id": str(generation["id"]),
                        "provider": provider.name,
                        "model": provider.model,
                        "text": "".join(content),
                        "input_tokens": usage[0],
                        "output_tokens": usage[1],
                    }
                    committed = await ledger.settle(generation, "completed", result=result)
                    completed = True
                    if not committed:
                        yield event("error", code="lease_expired")
                        return
                    yield event("usage", input_tokens=usage[0], output_tokens=usage[1])
                    yield event("done", **result)
                    return
                except (providers.ProviderError, httpx.HTTPError) as error:
                    retryable = getattr(error, "retryable", True)
                    rejected = getattr(error, "rejected", False)
                    await ledger.finish_attempt(
                        current_attempt,
                        "failed",
                        error=type(error).__name__,
                        definite_rejection=rejected,
                    )
                    await circuit.report(permit, False)
                    current_attempt = None
                    # После первого фрагмента ответ уже виден клиенту. Смешивать его с другой моделью нельзя.
                    if content or not retryable:
                        failure = (
                            "Stream interrupted" if content else "Provider rejected the request"
                        )
                        break
            else:
                failure = "No available provider completed the request"
        status = "failed"
        await ledger.settle(generation, status, error=failure)
        completed = True
        yield event("error", code="upstream_failed", message=failure)
    except TimeoutError:
        status = "failed"
        failure = "Overall request deadline exceeded"
        yield event("error", code="deadline_exceeded")
    finally:
        if not completed:
            # Отмена ASGI не должна прервать освобождение бюджета и запись неопределённого расхода.
            with anyio.CancelScope(shield=True):
                if current_attempt:
                    await ledger.finish_attempt(current_attempt, status, error=failure)
                if permit:
                    if status == "cancelled":
                        await circuit.release(permit)
                    else:
                        await circuit.report(permit, False)
                await ledger.settle(generation, status, error=failure)
