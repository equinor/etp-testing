import io
import uuid
import time
import asyncio
import json
import datetime
import warnings
import pprint
import lxml.etree as ET

import httpx
import websockets
import fastavro
import numpy as np


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


async def query_localhost(message):
    uri = "ws://localhost:9002"
    async with websockets.connect(
        uri,
        subprotocols=["etp12.energistics.org"],
    ) as ws:
        print(ws.open)
        await ws.send(message)
        return await ws.recv()


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


# mh_record = dict(
#     protocol=0,  # Core protocol
#     messageType=1,  # RequestSession
#     correlationId=0,  # Ignored for RequestSession
#     messageId=2,  # non-zero, even numbers for the client
#     messageFlags=0x2,  # FIN-bit
# )
#
# rs_record = dict(
#     applicationName="oycli",
#     applicationVersion="0.0.1",
#     clientInstanceId=uuid.uuid4().bytes,
#     requestedProtocols=[
#         dict(  # SupportedProtocol
#             protocol=3,
#             protocolVersion=dict(
#                 major=1,
#                 minor=2,
#             ),
#             role="store",
#         ),
#     ],
#     supportedDataObjects=[
#         dict(  # SupportedDataObject
#             qualifiedType="resqml20.*",
#         ),
#     ],
#     currentDateTime=datetime.datetime.now(datetime.timezone.utc).timestamp(),
#     earliestRetainedChangeTime=0,
#     serverAuthorizationRequired=False,
# )
#
# asyncio.run(hello_localhost())
#
# fo = io.BytesIO()
# # Write the message header
# fastavro.write.schemaless_writer(fo, etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"], mh_record)
# # Write the request session body
# fastavro.write.schemaless_writer(fo, etp_schemas["Energistics.Etp.v12.Protocol.Core.RequestSession"], rs_record)
#
# resp = asyncio.run(query_localhost(fo.getvalue()))
#
# fo = io.BytesIO(resp)
# record = fastavro.read.schemaless_reader(
#     fo,
#     etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"],
#     return_record_name=True,
# )
# pprint.pprint(record)
# record = fastavro.read.schemaless_reader(
#     fo,
#     etp_schemas["Energistics.Etp.v12.Protocol.Core.OpenSession"],
#     return_record_name=True,
# )
# pprint.pprint(record)


async def start_and_stop(url="ws://localhost:9002", headers={}):
    print(f"Talking to: {url}")
    async with websockets.connect(
        url,
        extra_headers=headers,
        subprotocols=["etp12.energistics.org"],
    ) as ws:
        MSG_ID = 2

        # TODO: Figure out how set websocket max sizes
        # Request session, i.e., start the thingy
        mh_record = dict(
            protocol=0,  # Core protocol
            messageType=1,  # RequestSession
            correlationId=0,  # Ignored for RequestSession
            messageId=(MSG_ID := MSG_ID + 2),  # non-zero, even numbers for the client
            messageFlags=0x2,  # FIN-bit
        )

        rs_record = dict(
            applicationName="oycli",
            applicationVersion="0.0.1",
            clientInstanceId=uuid.uuid4().bytes,
            requestedProtocols=[  # [SupportedProtocol]
                dict(
                    protocol=3,  # Discovery
                    protocolVersion=dict(
                        major=1,
                        minor=2,
                    ),
                    role="store",
                ),
                dict(
                    protocol=4,  # Store
                    protocolVersion=dict(
                        major=1,
                        minor=2,
                    ),
                    role="store",
                ),
                dict(
                    protocol=9,  # DataArray
                    protocolVersion=dict(
                        major=1,
                        minor=2,
                    ),
                    role="store",
                ),
                dict(
                    protocol=24,  # Dataspace
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
            currentDateTime=datetime.datetime.now(datetime.timezone.utc).timestamp(),
            earliestRetainedChangeTime=0,
            serverAuthorizationRequired=False,
        )

        fo = io.BytesIO()
        # Write the message header
        fastavro.write.schemaless_writer(
            fo, etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"], mh_record
        )
        # Write the request session body
        fastavro.write.schemaless_writer(
            fo,
            etp_schemas["Energistics.Etp.v12.Protocol.Core.RequestSession"],
            rs_record,
        )

        await ws.send(fo.getvalue())
        resp = await ws.recv()

        fo = io.BytesIO(resp)
        record = fastavro.read.schemaless_reader(
            fo,
            etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"],
            return_record_name=True,
        )
        pprint.pprint(record)
        record = fastavro.read.schemaless_reader(
            fo,
            etp_schemas["Energistics.Etp.v12.Protocol.Core.OpenSession"],
            return_record_name=True,
        )
        pprint.pprint(record)

        # Query dataspaces
        mh_record = dict(
            protocol=24,  # Dataspace
            messageType=1,  # GetDataspaces
            correlationId=0,  # Ignored
            messageId=(MSG_ID := MSG_ID + 2),
            messageFlags=0x2,  # Fin
        )
        gd_record = dict(
            storeLastWriteFilter=None,  # Include all dataspaces
        )

        fo = io.BytesIO()
        fastavro.write.schemaless_writer(
            fo, etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"], mh_record
        )
        fastavro.write.schemaless_writer(
            fo,
            etp_schemas["Energistics.Etp.v12.Protocol.Dataspace.GetDataspaces"],
            gd_record,
        )

        # The final response message should have the fin-bit set in the message header
        await ws.send(fo.getvalue())

        while True:
            resp = await ws.recv()

            fo = io.BytesIO(resp)
            mh_record = fastavro.read.schemaless_reader(
                fo,
                etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"],
                return_record_name=True,
            )
            pprint.pprint(mh_record)

            if mh_record["messageType"] == 1000:
                record = fastavro.read.schemaless_reader(
                    fo,
                    etp_schemas["Energistics.Etp.v12.Protocol.Core.ProtocolException"],
                    return_record_name=True,
                )
            else:
                record = fastavro.read.schemaless_reader(
                    fo,
                    etp_schemas[
                        "Energistics.Etp.v12.Protocol.Dataspace.GetDataspacesResponse"
                    ],
                    return_record_name=True,
                )
            pprint.pprint(record)

            if (mh_record["messageFlags"] & 0x2) != 0:
                # We have received a FIN-bit, i.e., the last reponse has been
                # read.
                print("Last message read from this protocol")
                break

        # Get resources
        mh_record = dict(
            protocol=3,  # Store
            messageType=1,  # GetResources
            correlationId=0,  # Ignored
            messageId=(MSG_ID := MSG_ID + 2),
            messageFlags=0x2,
        )
        gr_record = dict(  # GetResources
            context=dict(  # ContextInfo
                uri="eml:///dataspace('demo/pss-rand')",
                depth=2,
                navigableEdges="Primary",
            ),
            scope="targets",
            storeLastWriteFilter=None,
            activeStatusFilter=None,  # perhaps active?
        )

        fo = io.BytesIO()
        fastavro.write.schemaless_writer(
            fo, etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"], mh_record
        )
        fastavro.write.schemaless_writer(
            fo,
            etp_schemas["Energistics.Etp.v12.Protocol.Discovery.GetResources"],
            gr_record,
        )

        await ws.send(fo.getvalue())

        uris = []
        while True:
            resp = await ws.recv()

            fo = io.BytesIO(resp)
            mh_record = fastavro.read.schemaless_reader(
                fo,
                etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"],
                return_record_name=True,
            )
            pprint.pprint(mh_record)

            if mh_record["messageType"] == 1000:
                record = fastavro.read.schemaless_reader(
                    fo,
                    etp_schemas["Energistics.Etp.v12.Protocol.Core.ProtocolException"],
                    return_record_name=True,
                )
            else:
                record = fastavro.read.schemaless_reader(
                    fo,
                    etp_schemas[
                        "Energistics.Etp.v12.Protocol.Discovery.GetResourcesResponse"
                    ],
                )
                uris = [res["uri"] for res in record["resources"]]
            pprint.pprint(record)

            if (mh_record["messageFlags"] & 0x2) != 0:
                # We have received a FIN-bit, i.e., the last reponse has been
                # read.
                print("Last message read from this protocol")
                break

        print(uris)

        epc_uri = list(filter(lambda x: "EpcExternalPartReference" in x, uris))[0]
        print(epc_uri)

        # Get data objects
        # In case of small websocket packages, this can be sent (or returned) as chunks.
        mh_record = dict(
            protocol=4,  # Store
            messageType=1,  # GetDataObjects
            correlationId=0,  # Ignored
            messageId=(MSG_ID := MSG_ID + 2),
            messageFlags=0x2,  # Multi-part=False
        )
        gdo_record = dict(
            uris=dict((uri, uri) for uri in uris),
        )

        fo = io.BytesIO()
        fastavro.write.schemaless_writer(
            fo, etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"], mh_record
        )
        fastavro.write.schemaless_writer(
            fo,
            etp_schemas["Energistics.Etp.v12.Protocol.Store.GetDataObjects"],
            gdo_record,
        )

        await ws.send(fo.getvalue())

        uris = []
        roots = []
        while True:
            resp = await ws.recv()

            fo = io.BytesIO(resp)
            mh_record = fastavro.read.schemaless_reader(
                fo,
                etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"],
                return_record_name=True,
            )
            pprint.pprint(mh_record)

            if mh_record["messageType"] == 1000:
                record = fastavro.read.schemaless_reader(
                    fo,
                    etp_schemas["Energistics.Etp.v12.Protocol.Core.ProtocolException"],
                    return_record_name=True,
                )
            else:
                record = fastavro.read.schemaless_reader(
                    fo,
                    etp_schemas[
                        "Energistics.Etp.v12.Protocol.Store.GetDataObjectsResponse"
                    ],
                )
                uris = [
                    record["dataObjects"][obj]["resource"]["uri"]
                    for obj in list(record["dataObjects"])
                ]
                roots = [
                    ET.fromstring(record["dataObjects"][obj]["data"])
                    for obj in list(record["dataObjects"])
                ]

            # pprint.pprint(record)

            if (mh_record["messageFlags"] & 0x2) != 0:
                # We have received a FIN-bit, i.e., the last reponse has been
                # read.
                print("Last message read from this protocol")
                break

        pir = {}
        for uri, root in zip(uris, roots):
            res = root.xpath("//*[starts-with(local-name(), 'PathInHdfFile')]")
            if len(res) == 1:
                print(uri, root, res[0].text)
                pir[uri] = res[0].text

        # Get data array metadata
        # TODO: Figure out how to do more sophisticated querying
        mh_record = dict(
            protocol=9,  # DataArray
            messageType=6,  # GetDataArrayMetadata
            correlationId=0,  # Ignored
            messageId=(MSG_ID := MSG_ID + 2),
            messageFlags=0x2,  # Multi-part=False
        )
        gdam_record = dict(
            dataArrays={
                # XXX: Note that we use the pathInResource (pathInHdfFile) from
                # the Grid2dRepresentation, but the uri is for
                # EpcExternalPartReference!
                key: dict(
                    uri=epc_uri,
                    # pathInResource="/RESQML/49e4c1e1-891c-11ee-a655-4da4d0f45a8a/zvalues",
                    pathInResource=val,
                )
                for key, val in pir.items()
            },
        )

        fo = io.BytesIO()
        fastavro.write.schemaless_writer(
            fo, etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"], mh_record
        )
        fastavro.write.schemaless_writer(
            fo,
            etp_schemas["Energistics.Etp.v12.Protocol.DataArray.GetDataArrayMetadata"],
            gdam_record,
        )

        await ws.send(fo.getvalue())

        dimensions = []
        while True:
            resp = await ws.recv()

            fo = io.BytesIO(resp)
            mh_record = fastavro.read.schemaless_reader(
                fo,
                etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"],
                return_record_name=True,
            )
            pprint.pprint(mh_record)

            if mh_record["messageType"] == 1000:
                record = fastavro.read.schemaless_reader(
                    fo,
                    etp_schemas["Energistics.Etp.v12.Protocol.Core.ProtocolException"],
                    return_record_name=True,
                )
            else:
                record = fastavro.read.schemaless_reader(
                    fo,
                    etp_schemas[
                        "Energistics.Etp.v12.Protocol.DataArray.GetDataArrayMetadataResponse"
                    ],
                )
                dimensions = record["arrayMetadata"][sorted(pir)[0]]["dimensions"]
            pprint.pprint(record)

            if (mh_record["messageFlags"] & 0x2) != 0:
                # We have received a FIN-bit, i.e., the last reponse has been
                # read.
                print("Last message read from this protocol")
                break

        # Get full data array
        mh_record = dict(
            protocol=9,  # DataArray
            messageType=2,  # GetDataArrays
            correlationId=0,  # Ignored
            messageId=(MSG_ID := MSG_ID + 2),
            messageFlags=0x2,  # Multi-part=False
        )
        gda_record = dict(
            dataArrays={
                # XXX: Note that we use the pathInResource (pathInHdfFile) from
                # the Grid2dRepresentation, but the uri is for
                # EpcExternalPartReference!
                key: dict(
                    uri=epc_uri,
                    # pathInResource="/RESQML/49e4c1e1-891c-11ee-a655-4da4d0f45a8a/zvalues",
                    pathInResource=val,
                )
                for key, val in pir.items()
            },
        )

        fo = io.BytesIO()
        fastavro.write.schemaless_writer(
            fo, etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"], mh_record
        )
        fastavro.write.schemaless_writer(
            fo,
            etp_schemas["Energistics.Etp.v12.Protocol.DataArray.GetDataArrays"],
            gdam_record,
        )

        await ws.send(fo.getvalue())

        data = []
        while True:
            resp = await ws.recv()

            fo = io.BytesIO(resp)
            mh_record = fastavro.read.schemaless_reader(
                fo,
                etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"],
                return_record_name=True,
            )
            pprint.pprint(mh_record)

            if mh_record["messageType"] == 1000:
                record = fastavro.read.schemaless_reader(
                    fo,
                    etp_schemas["Energistics.Etp.v12.Protocol.Core.ProtocolException"],
                    return_record_name=True,
                )
            else:
                record = fastavro.read.schemaless_reader(
                    fo,
                    etp_schemas[
                        "Energistics.Etp.v12.Protocol.DataArray.GetDataArraysResponse"
                    ],
                )
                # Note that GetDataArraysResponse includes the dimensions as well
                data = record["dataArrays"][sorted(pir)[0]]["data"]["item"]["values"]
            pprint.pprint(record)

            if (mh_record["messageFlags"] & 0x2) != 0:
                # We have received a FIN-bit, i.e., the last reponse has been
                # read.
                print("Last message read from this protocol")
                break

        data = np.asarray(data).reshape(dimensions[::-1])
        print(data.shape)

        # Get first 5 rows of the data array
        mh_record = dict(
            protocol=9,  # DataArray
            messageType=3,  # GetDataSubrrays
            correlationId=0,  # Ignored
            messageId=(MSG_ID := MSG_ID + 2),
            messageFlags=0x2,  # Multi-part=False
        )
        gds_record = dict(
            dataSubarrays={
                key: dict(
                    uid=dict(
                        uri=epc_uri,
                        pathInResource=val,
                    ),
                    starts=[0, 0],
                    counts=[12, 5],
                )
                for key, val in pir.items()
            },
        )

        fo = io.BytesIO()
        fastavro.write.schemaless_writer(
            fo, etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"], mh_record
        )
        fastavro.write.schemaless_writer(
            fo,
            etp_schemas["Energistics.Etp.v12.Protocol.DataArray.GetDataSubarrays"],
            gds_record,
        )

        await ws.send(fo.getvalue())

        subdata = []
        while True:
            resp = await ws.recv()

            fo = io.BytesIO(resp)
            mh_record = fastavro.read.schemaless_reader(
                fo,
                etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"],
                return_record_name=True,
            )
            pprint.pprint(mh_record)

            if mh_record["messageType"] == 1000:
                record = fastavro.read.schemaless_reader(
                    fo,
                    etp_schemas["Energistics.Etp.v12.Protocol.Core.ProtocolException"],
                    return_record_name=True,
                )
            else:
                record = fastavro.read.schemaless_reader(
                    fo,
                    etp_schemas[
                        "Energistics.Etp.v12.Protocol.DataArray.GetDataSubarraysResponse"
                    ],
                )
                pprint.pprint(record)
                # Note that GetDataArraysResponse includes the dimensions as well
                subdata = np.asarray(
                    record["dataSubarrays"][sorted(pir)[0]]["data"]["item"]["values"]
                ).reshape(record["dataSubarrays"][sorted(pir)[0]]["dimensions"][::-1])
            pprint.pprint(record)

            if (mh_record["messageFlags"] & 0x2) != 0:
                # We have received a FIN-bit, i.e., the last reponse has been
                # read.
                print("Last message read from this protocol")
                break

        # Get the last 6 rows of the data array
        mh_record = dict(
            protocol=9,  # DataArray
            messageType=3,  # GetDataSubrrays
            correlationId=0,  # Ignored
            messageId=(MSG_ID := MSG_ID + 2),
            messageFlags=0x2,  # Multi-part=False
        )
        gds_record = dict(
            dataSubarrays={
                key: dict(
                    uid=dict(
                        uri=epc_uri,
                        pathInResource=val,
                    ),
                    starts=[0, 5],
                    counts=[12, 6],
                )
                for key, val in pir.items()
            },
        )

        fo = io.BytesIO()
        fastavro.write.schemaless_writer(
            fo, etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"], mh_record
        )
        fastavro.write.schemaless_writer(
            fo,
            etp_schemas["Energistics.Etp.v12.Protocol.DataArray.GetDataSubarrays"],
            gds_record,
        )

        await ws.send(fo.getvalue())

        subdata_2 = []
        while True:
            resp = await ws.recv()

            fo = io.BytesIO(resp)
            mh_record = fastavro.read.schemaless_reader(
                fo,
                etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"],
                return_record_name=True,
            )
            pprint.pprint(mh_record)

            if mh_record["messageType"] == 1000:
                record = fastavro.read.schemaless_reader(
                    fo,
                    etp_schemas["Energistics.Etp.v12.Protocol.Core.ProtocolException"],
                    return_record_name=True,
                )
            else:
                record = fastavro.read.schemaless_reader(
                    fo,
                    etp_schemas[
                        "Energistics.Etp.v12.Protocol.DataArray.GetDataSubarraysResponse"
                    ],
                )
                pprint.pprint(record)
                # Note that GetDataArraysResponse includes the dimensions as well
                subdata_2 = np.asarray(
                    record["dataSubarrays"][sorted(pir)[0]]["data"]["item"]["values"]
                ).reshape(record["dataSubarrays"][sorted(pir)[0]]["dimensions"][::-1])
            pprint.pprint(record)

            if (mh_record["messageFlags"] & 0x2) != 0:
                # We have received a FIN-bit, i.e., the last reponse has been
                # read.
                print("Last message read from this protocol")
                break

        subdata_rec = np.concatenate([subdata, subdata_2], axis=0)
        np.testing.assert_allclose(sorted(subdata_rec.ravel()), sorted(data.ravel()))

        print(subdata.shape, data[:, :5].shape, data[:5].shape)
        print(data)
        print("\n\n")
        print(subdata)

        print("\n\n")
        print(subdata_2)

        np.testing.assert_allclose(sorted(subdata.ravel()), sorted(data[:5].ravel()))
        np.testing.assert_allclose(subdata, data[:5])

        np.testing.assert_allclose(subdata_rec, data)

        # Close session
        mh_record = dict(
            protocol=0,  # Core
            messageType=5,  # CloseSession
            correlationId=0,  # Ignored
            messageId=(MSG_ID := MSG_ID + 2),
            messageFlags=0x2,
        )

        cs_record = dict(
            reason="Because I said so",
        )

        fo = io.BytesIO()
        fastavro.write.schemaless_writer(
            fo, etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"], mh_record
        )
        fastavro.write.schemaless_writer(
            fo, etp_schemas["Energistics.Etp.v12.Protocol.Core.CloseSession"], cs_record
        )

        await ws.send(fo.getvalue())

        await ws.wait_closed()
        assert ws.closed
        print(ws.close_reason)


# TODO: Populate localhost with data
asyncio.run(start_and_stop())


# asyncio.run(
#     start_and_stop(
#         url="wss://interop-rddms.azure-api.net",
#         headers={"Authorization": "Bearer " + get_token()},
#     ),
# )
