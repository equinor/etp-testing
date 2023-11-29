import time
import asyncio
import websockets
import json
import datetime
import warnings


def get_token(token_filename: str = ".token") -> str:
    with open(".token", "r") as f:
        f_r = json.load(f)
        token = f_r.get("accessToken")
        expiration = datetime.datetime.fromtimestamp(f_r.get("expires_on"))

        if expiration < datetime.datetime.now():
            warnings.warn("Token has expired")

        return token


async def hello_azurehost(token: str):
    url = "wss://interop-rddms.azure-api.net"
    header = {"Authorization": "Bearer " + token}
    async with websockets.connect(
        url,
        subprotocols=["etp12.energistics.org"],
        extra_headers={**header},
    ) as ws:
        print(ws.open)
        ping = await ws.ping()
        print(await ping)


# asyncio.run(hello_azurehost(get_token()))


async def hello_localhost():
    uri = "ws://localhost:9002"
    async with websockets.connect(
        uri,
        subprotocols=["etp12.energistics.org"],
    ) as ws:
        print(ws.open)
        ping = await ws.ping()
        print(await ping)


with open("etp.avpr", "r") as f:
    avro_schemas = json.load(f)

print(avro_schemas)

asyncio.run(hello_localhost())
