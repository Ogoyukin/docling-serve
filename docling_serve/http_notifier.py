import asyncio
import hashlib
import hmac
import json
import logging
import time
from typing import Any

import httpx

from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.base_notifier import BaseNotifier
from docling_jobkit.orchestrators.base_orchestrator import BaseOrchestrator

from docling_serve.datamodel.webhook import WebhookConfig

_log = logging.getLogger(__name__)


class HttpNotifier(BaseNotifier):
    """Send webhook callbacks when tasks reach a terminal state."""

    def __init__(self, orchestrator: BaseOrchestrator):
        super().__init__(orchestrator)
        self._webhook_configs: dict[str, WebhookConfig] = {}
        self._dispatched: set[str] = set()

    async def add_task(self, task_id: str) -> None:  # pragma: no cover - interface parity
        self._dispatched.discard(task_id)

    async def remove_task(self, task_id: str) -> None:  # pragma: no cover - interface parity
        self._webhook_configs.pop(task_id, None)
        self._dispatched.discard(task_id)

    async def notify_task_subscribers(self, task_id: str) -> None:
        config = self._webhook_configs.get(task_id)
        if config is None or task_id in self._dispatched:
            return

        try:
            task = await self.orchestrator.task_status(task_id=task_id)
        except Exception as exc:  # pragma: no cover - defensive
            _log.error("Failed to load task %s for webhook: %s", task_id, exc)
            return

        if not task.is_completed():
            return

        payload = self._build_payload(task_id, task)
        self._dispatched.add(task_id)
        asyncio.create_task(self._dispatch(task_id, config, payload))

    async def notify_queue_positions(self) -> None:  # pragma: no cover - interface parity
        return

    def register_webhook(self, task_id: str, config: WebhookConfig | None) -> None:
        if config is None:
            return
        self._webhook_configs[task_id] = config

    def _build_payload(self, task_id: str, task: Any) -> dict[str, Any]:
        meta = task.processing_meta
        if hasattr(meta, "model_dump"):
            meta = meta.model_dump()
        locator = None
        result_keys = getattr(self.orchestrator, "_task_result_keys", {})
        if isinstance(result_keys, dict):
            locator = result_keys.get(task_id)
        return {
            "task_id": task.task_id,
            "status": task.task_status.value
            if isinstance(task.task_status, TaskStatus)
            else str(task.task_status),
            "type": task.task_type.value if hasattr(task.task_type, "value") else str(task.task_type),
            "processing_meta": meta,
            "result_locator": locator,
        }

    async def _dispatch(self, task_id: str, config: WebhookConfig, payload: dict[str, Any]) -> None:
        timestamp = str(int(time.time()))
        body = json.dumps(payload)
        headers = {"Content-Type": "application/json", **config.headers}
        signature_header = None
        if config.secret:
            message = f"{timestamp}.{body}".encode()
            digest = hmac.new(config.secret.encode(), message, hashlib.sha256).hexdigest()
            signature_header = digest
            headers["X-Docling-Signature"] = digest
        headers["X-Docling-Timestamp"] = timestamp

        attempts = max(config.max_retries, 0) + 1
        backoff = max(config.backoff_factor, 0.0)

        async with httpx.AsyncClient() as client:
            for attempt in range(attempts):
                try:
                    response = await client.request(
                        method=config.method,
                        url=str(config.url),
                        content=body,
                        headers=headers,
                        timeout=10.0,
                    )
                    if 200 <= response.status_code < 300:
                        _log.info(
                            "Webhook for task %s delivered with status %s", task_id, response.status_code
                        )
                        return
                    _log.warning(
                        "Webhook for task %s failed with status %s: %s",
                        task_id,
                        response.status_code,
                        response.text,
                    )
                except Exception as exc:
                    _log.error("Webhook attempt for task %s failed: %s", task_id, exc)

                if attempt < attempts - 1:
                    sleep_for = backoff * (2**attempt)
                    await asyncio.sleep(sleep_for)

        _log.error(
            "Exhausted webhook retries for task %s (signature=%s)", task_id, signature_header or "<none>"
        )
