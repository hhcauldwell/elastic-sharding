import asyncio
import logging
import socket
import sys
import traceback

from aiohttp import web
from functools import partial

from cluster import Cluster


port = int(sys.argv[1])
hostname = socket.gethostname()
zk_servers = "localhost"


logging.basicConfig(level=logging.INFO)


routes = web.RouteTableDef()


async def server_ready(request):
    if not request.app.get("ready", False):
        raise web.HTTPServiceUnavailable(reason="Server hasn't started yet.")


async def cluster_ready(request):
    timeout = 0.1
    try:
        await asyncio.wait_for(request.app["cluster"].ready(), timeout)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        raise web.HTTPServiceUnavailable(reason="Cluster not ready!")


@routes.get("/ready")
async def ready_check_handler(request) -> None:
    await server_ready(request)
    await cluster_ready(request)
    return web.Response(text="Ready check OK!")


@routes.get("/shards")
async def list_shards_handler(request) -> None:
    return web.json_response(request.app["cluster"].allocations)


@web.middleware
async def error_middleware(request: web.Request, handler):
    try:
        return await handler(request)
    except web.HTTPException as exc:
        request.app["log"].exception(request.raw_path)
        return web.json_response({"reason": exc.reason}, status=exc.status)
    except Exception:
        request.app["log"].exception(request.raw_path)
        return web.json_response(
            {"reason": traceback.format_exception(*sys.exc_info())},
            status=500,
        )


def handle_async_exc(task):
    global asyncio
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        logging.exception(exc)


async def on_rebalance_start(app):
    app["log"].warning("Rebalancing cluster")


async def on_rebalance_end(app, shard, allocations):
    app["log"].warning(f"Cluster rebalanced {shard} of {len(allocations)}")


async def on_startup_handler(app) -> None:
    app["log"].info("Server starting")

    app["cluster"] = Cluster(
        hostname,
        port,
        zk_servers,
        on_rebalance_start=partial(on_rebalance_start, app),
        on_rebalance_end=partial(on_rebalance_end, app),
        rebalance_timeout=5.0,
    )
    await app["cluster"].start()

    app["ready"] = True
    app["log"].info("Server ready.")


async def on_shutdown_handler(app) -> None:
    app["log"].info("Server shutting down")
    if "cluster" in app:
        await app["cluster"].stop()


if __name__ == "__main__":
    app = web.Application(middlewares=[error_middleware])
    app["host"] = hostname
    app["log"] = logging.getLogger(f"{hostname}:{port}")
    app.router.add_routes(routes)
    app.on_startup.append(on_startup_handler)
    app.on_shutdown.append(on_shutdown_handler)
    web.run_app(app, port=port)
