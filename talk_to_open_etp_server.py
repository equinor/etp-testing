import asyncio
import websockets

async def hello():
    uri = "ws://localhost:9002"
    async with websockets.connect(
        uri,
        subprotocols=["etp12.energistics.org"],
    ) as ws:
        print(ws.open)
        ping = await ws.ping()
        print(await ping)


asyncio.run(hello())
