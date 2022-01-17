import asyncio
import logging

from aiosignal import Signal
from aiozk import ZKClient


log = logging.getLogger(__name__)


class Cluster:

    def __init__(
        self,
        host: str,
        port: int,
        zk_servers: str = "localhost",
        on_rebalance_start=None,
        on_rebalance_end=None,
        rebalance_delay: float = 1.0,
        rebalance_timeout: float = 5.0,
    ):
        self.host = host
        self.port = port
        self.name = f"{self.host}:{self.port}"
        self.zk_servers = zk_servers
        self.rebalance_delay = rebalance_delay
        self.rebalance_timeout = rebalance_timeout

        self.zk = ZKClient(self.zk_servers)

        self.shard = None
        self.party = None
        self.allocations = dict()

        self.rebalance_task = None

        self.on_rebalance_start = Signal(self)
        self.on_rebalance_start.append(on_rebalance_start)
        self.on_rebalance_start.freeze()
        self.on_rebalance_end = Signal(self)
        self.on_rebalance_end.append(on_rebalance_end)
        self.on_rebalance_end.freeze()

    @property
    def party_path(self):
        return "/sec/party"

    @property
    def gate_path(self):
        return "/sec/gate"

    async def ready(self):
        if self.shard is None:
            raise Exception("No shard allocated.")
        await self.zk.get("/")

    async def rebalance(self):
        self.shard = None
        self.allocations.clear()

        await self.on_rebalance_start.send()

        gate = self.zk.recipes.DoubleBarrier(
            self.gate_path,
            len(self.party.members)
        )
        await gate.enter()

        new_allocations = {m: i for i, m in enumerate(self.party.members)}
        try:
            new_members = self.party.members.copy()
            new_members.sort()
            new_allocations = {m: i for i, m in enumerate(new_members)}
        finally:
            await gate.leave()

        self.shard = new_allocations.get(self.name, None)
        log.warning(self.shard)
        self.allocations = new_allocations
        await self.on_rebalance_end.send(self.shard, self.allocations)

    async def rebalance_loop(self):
        timeout = self.rebalance_timeout
        await asyncio.sleep(self.rebalance_delay)
        while True:
            try:
                await asyncio.wait_for(self.rebalance(), timeout=timeout)
                return
            except asyncio.TimeoutError:
                log.error(f"Rebalance timeout: timeout={timeout}")

    def rebalance_done(self, task):
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            # format
            log.exception(exc)

    def stop_rebalance(self):
        if self.rebalance_task and not self.rebalance_task.done():
            self.rebalance_task.cancel()

    def trigger_rebalance(self, data):
        self.stop_rebalance()
        self.rebalance_task = asyncio.create_task(self.rebalance_loop())
        self.rebalance_task.add_done_callback(self.rebalance_done)

    async def start(self):
        await self.zk.start()
        self.party = self.zk.recipes.Party(self.party_path, self.name)
        self.party.watcher.add_callback(
            self.party.base_path,
            self.trigger_rebalance,
        )
        await self.party.join()

    async def stop(self):
        if self.party:
            self.party.watcher.remove_callback(
                self.party.base_path,
                self.trigger_rebalance,
            )
        self.stop_rebalance()
        await self.zk.close()
