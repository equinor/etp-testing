import uuid
import time
import asyncio
import json
import datetime
import warnings
import pprint

import httpx
import websockets
import fastavro


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


def get_azurehost_capabilities(token: str):
    # TODO: Why doesn't this work???
    url = "https://interop-rddms.azure-api.net/.well-known/etp-server-capabilities?GetVersion=etp12.energistics.org"
    headers = {"Authorization": "Bearer " + token}
    return httpx.get(url, headers=headers).raise_for_status().json()


# token = get_token()
# pprint.pprint(get_azurehost_capabilities(token))
#
# asyncio.run(hello_azurehost(token))
# wat


async def hello_localhost():
    uri = "ws://localhost:9002"
    async with websockets.connect(
        uri,
        subprotocols=["etp12.energistics.org"],
    ) as ws:
        print(ws.open)
        ping = await ws.ping()
        print(await ping)


def get_localhost_capabilities():
    url = "http://localhost:9002/.well-known/etp-server-capabilities?GetVersion=etp12.energistics.org"
    return httpx.get(url).raise_for_status().json()


# pprint.pprint(get_localhost_capabilities())
# wat


with open("etp.avpr", "r") as foo:
    jschema = json.load(foo)


def parse_func(js, named_schemas=dict()):
    ps = fastavro.schema.parse_schema(js, named_schemas)
    return fastavro.schema.fullname(ps), ps


etp_schemas = dict(parse_func(js) for js in jschema["types"])

mh_record = dict(
    protocol=0,  # Core protocol
    messageType=1,  # RequestSession
    correlationId=0,  # Ignored for RequestSession
    messageId=2,  # non-zero, even numbers for the client
    messageFlags=0x2,  # FIN-bit
)

rs_record = dict(
    applicationName="oycli",
    applicationVersion="0.0.1",
    clientInstanceId=uuid.uuid4().bytes,
    requestedProtocols=[
        dict(  # SupportedProtocol
            protocol=3,
            protocolVersion=dict(
                major=1,
                minor=2,
            ),
            role="store",
        ),
    ],
    supportedDataObjects=[
        dict(  # SupportedDataObject
            qualifiedType="resqml20.*",
        ),
    ],
    # currentDateTime=
)

asyncio.run(hello_localhost())
