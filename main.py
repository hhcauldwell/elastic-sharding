from aiohttp import web
from aiozk import ZKClient

import asyncio
import logging
import socket
import sys
import time
import traceback


port = 8080
hostname = socket.gethostname()
zk_servers = ""


logging.basicConfig(level=logging.INFO)


routes = web.RouteTableDef()


@routes.get("/ready")
async def ready_check_handler(request) -> None:
    timeout = 0.1

    if not request.app.get("ready", False):
        raise web.HTTPServiceUnavailable(reason="Server hasn't started yet.")

    try:
        await asyncio.wait_for(app["zk"].exists("/"), timeout)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        raise web.HTTPServiceUnavailable(reason="Zookeeper not ready!")

    return web.Response(text="Ready check OK!")


@routes.get("/shards")
async def list_shards_handler(request) -> None:
    return web.json_response(request.app["shards"])


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


async def on_startup_handler(app) -> None:
    app["log"].info("Server starting")
    app["shards"] = dict()

    app["zk"] = ZKClient(zk_servers)
    app["log"].info(f"Connecting to zookeeper '{zk_servers}': '{zk_servers}'")
    await app["zk"].start()

    app["ready"] = True
    app["log"].info("Server ready.")


async def on_shutdown_handler(app) -> None:
    app["log"].info("Server shutting down")


if __name__ == "__main__":
    app = web.Application(middlewares=[error_middleware])
    app["log"] = logging.getLogger(f"{hostname}:{port}")
    app.router.add_routes(routes)
    app.on_startup.append(on_startup_handler)
    app.on_shutdown.append(on_shutdown_handler)
    web.run_app(app, port=port)
