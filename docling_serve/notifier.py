import asyncio
from typing import Any

from docling_jobkit.orchestrators.base_notifier import BaseNotifier
from docling_jobkit.orchestrators.base_orchestrator import BaseOrchestrator

from docling_serve.http_notifier import HttpNotifier
from docling_serve.websocket_notifier import WebsocketNotifier


class MultiNotifier(BaseNotifier):
    def __init__(self, orchestrator: BaseOrchestrator, *notifiers: BaseNotifier):
        super().__init__(orchestrator)
        self.notifiers = list(notifiers)
        self.websocket_notifier = next(
            (n for n in self.notifiers if isinstance(n, WebsocketNotifier)), None
        )
        self.task_subscribers = (
            self.websocket_notifier.task_subscribers if self.websocket_notifier else {}
        )

    async def add_task(self, task_id: str):
        await asyncio.gather(*(n.add_task(task_id) for n in self.notifiers))

    async def remove_task(self, task_id: str):
        await asyncio.gather(*(n.remove_task(task_id) for n in self.notifiers))

    async def notify_task_subscribers(self, task_id: str):
        await asyncio.gather(*(n.notify_task_subscribers(task_id) for n in self.notifiers))

    async def notify_queue_positions(self):
        await asyncio.gather(*(n.notify_queue_positions() for n in self.notifiers))

    def register_webhook(self, task_id: str, config: Any) -> None:
        for notifier in self.notifiers:
            if isinstance(notifier, HttpNotifier):
                notifier.register_webhook(task_id, config)
