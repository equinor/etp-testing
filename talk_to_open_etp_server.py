import re
import sys
import io
import uuid
import time
import asyncio
import json
import datetime
import warnings
import pprint
import enum

# Third-party imports
import lxml.etree as ET
import httpx
import websockets
import fastavro
import numpy as np
import resqpy
import resqpy.model


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
# asyncio.run(hello_azurehost(token))
# wat


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


class MHFlags(enum.Enum):
    # Flags in MessageHeader, see section 23.25 in the ETP 1.2 standard
    FIN = 0x2
    COMPRESSED = 0x8
    ACK = 0x10
    HEADER_EXTENSION = 0x20


class ClientMessageId:
    lock = asyncio.Lock()
    # Unique, positive, increasing, even integers for the client
    message_id = 2

    @staticmethod
    async def get_next_id():
        async with ClientMessageId.lock:
            ret_id = ClientMessageId.message_id
            ClientMessageId.message_id += 2
            return ret_id


def serialize_message(header_record, body_record, body_schema_key):
    # TODO: Possibly support compression?
    fo = io.BytesIO()
    fastavro.write.schemaless_writer(
        fo, etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"], header_record
    )
    fastavro.write.schemaless_writer(fo, etp_schemas[body_schema_key], body_record)

    return fo.getvalue()


async def close_session(ws, reason):
    # Close session
    mh_record = dict(
        protocol=0,  # Core
        messageType=5,  # CloseSession
        correlationId=0,  # Ignored
        messageId=await ClientMessageId.get_next_id(),
        messageFlags=MHFlags.FIN.value,
    )

    cs_record = dict(
        reason=reason,
    )

    await ws.send(
        serialize_message(
            mh_record, cs_record, "Energistics.Etp.v12.Protocol.Core.CloseSession"
        )
    )

    await ws.wait_closed()
    assert ws.closed
    print(f"Websocket close reason: {ws.close_reason}")


async def handle_multipart_response(ws, schema_key):
    # Note that we only handle ProtocolException errors for now
    records = []
    while True:
        response = await ws.recv()

        fo = io.BytesIO(response)
        mh_record = fastavro.read.schemaless_reader(
            fo,
            etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"],
            return_record_name=True,
        )

        if mh_record["messageType"] == 1000:
            # ProtocolException
            record = fastavro.read.schemaless_reader(
                fo,
                etp_schemas["Energistics.Etp.v12.Protocol.Core.ProtocolException"],
                return_record_name=True,
            )
            # Output error object
            pprint.pprint(record)
            await close_session(ws, reason=f"Error from protocol '{schema_key}'")
            sys.exit(1)
        else:
            record = fastavro.read.schemaless_reader(
                fo,
                etp_schemas[schema_key],
                return_record_name=True,
            )
            records.append(record)

        if (mh_record["messageFlags"] & MHFlags.FIN.value) != 0:
            # We have received a FIN-bit, i.e., the last reponse has been
            # read.
            break

    return records


async def get_dataspaces(ws, store_last_write_filter=None):
    # Query dataspaces
    mh_record = dict(
        protocol=24,  # Dataspace
        messageType=1,  # GetDataspaces
        correlationId=0,  # Ignored
        messageId=await ClientMessageId.get_next_id(),
        messageFlags=MHFlags.FIN.value,  # Fin
    )
    gd_record = dict(
        storeLastWriteFilter=None,  # Include all dataspaces
    )

    # The final response message should have the fin-bit set in the message header
    await ws.send(
        serialize_message(
            mh_record, gd_record, "Energistics.Etp.v12.Protocol.Dataspace.GetDataspaces"
        )
    )

    return await handle_multipart_response(
        ws, "Energistics.Etp.v12.Protocol.Dataspace.GetDataspacesResponse"
    )


async def put_dataspaces(ws, dataspaces):
    uris = list(
        map(lambda x: (x if "eml:///" in x else f"eml:///dataspace('{x}')"), dataspaces)
    )
    paths = list(
        map(lambda x: re.search(r"eml:///dataspace\('(.+)'\)", x).group(1), uris)
    )

    mh_record = dict(
        protocol=24,  # Dataspace
        messageType=3,  # PutDataspaces
        correlationId=0,  # Ignored
        messageId=await ClientMessageId.get_next_id(),
        messageFlags=MHFlags.FIN.value,
    )
    time = datetime.datetime.now(datetime.timezone.utc).timestamp()
    pds_record = dict(
        dataspaces=dict(
            (
                uri,
                dict(
                    uri=uri,
                    path=path,
                    # Here we create the dataspace for the first time, hence last write
                    # and created are the same
                    storeLastWrite=time,
                    storeCreated=time,
                ),
            )
            for uri, path in zip(uris, paths)
        )
    )
    pprint.pprint(pds_record)

    await ws.send(
        serialize_message(
            mh_record,
            pds_record,
            "Energistics.Etp.v12.Protocol.Dataspace.PutDataspaces",
        )
    )

    return await handle_multipart_response(
        ws,
        "Energistics.Etp.v12.Protocol.Dataspace.PutDataspacesResponse",
    )


async def delete_dataspaces(ws, dataspaces):
    uris = list(
        map(lambda x: (x if "eml:///" in x else f"eml:///dataspace('{x}')"), dataspaces)
    )

    mh_record = dict(
        protocol=24,  # Dataspace
        messageType=4,  # DeleteDataspaces
        correlationId=0,  # Ignored
        messageId=await ClientMessageId.get_next_id(),
        messageFlags=MHFlags.FIN.value,
    )
    dds_record = dict(uris=dict((uri, uri) for uri in uris))

    await ws.send(
        serialize_message(
            mh_record,
            dds_record,
            "Energistics.Etp.v12.Protocol.Dataspace.DeleteDataspaces",
        )
    )

    return await handle_multipart_response(
        ws, "Energistics.Etp.v12.Protocol.Dataspace.DeleteDataspacesResponse"
    )


async def start_and_stop(
    url="ws://localhost:9002", headers={}, dataspace="demo/rand-cli", verify=True
):
    print(f"Talking to: {url}")
    async with websockets.connect(
        url,
        extra_headers=headers,
        subprotocols=["etp12.energistics.org"],
    ) as ws:
        # TODO: Figure out how to set websocket max sizes
        # Request session, i.e., start the thingy
        mh_record = dict(
            protocol=0,  # Core protocol
            messageType=1,  # RequestSession
            correlationId=0,  # Ignored for RequestSession
            messageId=await ClientMessageId.get_next_id(),
            messageFlags=MHFlags.FIN.value,  # FIN-bit
        )

        rs_record = dict(
            applicationName="somecli",
            applicationVersion="0.0.0",
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

        # Read the response
        # TODO: Handle possible errors
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

        records = await get_dataspaces(ws, store_last_write_filter=None)

        if any(
            ds["path"] == dataspace for record in records for ds in record["dataspaces"]
        ):
            print(f"Deleting dataspace {dataspace}")
            records = await delete_dataspaces(ws, [dataspace])

            if verify:
                # Test to see that the dataspace really was deleted
                records = await get_dataspaces(ws, store_last_write_filter=None)
                assert not any(
                    ds["path"] == dataspace
                    for record in records
                    for ds in record["dataspaces"]
                )

        # Now 'dataspace' does not exist
        # Next, we create it
        records = await put_dataspaces(ws, [dataspace])
        if verify:
            # Test to see that the dataspace now exists
            records = await get_dataspaces(ws, store_last_write_filter=None)
            assert any(
                ds["path"] == dataspace
                for record in records
                for ds in record["dataspaces"]
            )

        # Create random test data
        model = resqpy.model.new_model("test.epc", quiet=False)
        # This needs to be done in order to get a uuid
        crs = resqpy.crs.Crs(model)
        crs.create_xml()
        z_values = np.random.random((3, 4))
        mesh = resqpy.surface.Mesh(
            model,
            crs_uuid=model.crs_uuid,
            mesh_flavour="reg&z",
            ni=z_values.shape[1],  # rows
            nj=z_values.shape[0],  # columns
            origin=(np.random.random(), np.random.random(), 0.0),
            dxyz_dij=np.array([[1.0, 0.0, 0.0], [0.0, 0.5, 0.0]]),
            z_values=z_values,
            title="Random test-data",
            originator="someon",
        )
        mesh.create_xml()
        print(model.roots()[0])
        # Check if we get the correct xml-document out
        print(ET.tostring(model.roots()[1]))
        # TODO: Check if we need to handle the external part reference manually
        # This might be handled by the server when it creates its own hdf5-file
        # print(model.external_parts_list())

        # Here 'dataspace' exists
        await close_session(ws, "🙉")
        wat

        # Get resources
        mh_record = dict(
            protocol=3,  # Store
            messageType=1,  # GetResources
            correlationId=0,  # Ignored
            messageId=await ClientMessageId.get_next_id(),
            messageFlags=MHFlags.FIN.value,
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

            if (mh_record["messageFlags"] & MHFlags.FIN.value) != 0:
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
            messageId=await ClientMessageId.get_next_id(),
            messageFlags=MHFlags.FIN.value,  # Multi-part=False
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

            if (mh_record["messageFlags"] & MHFlags.FIN.value) != 0:
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
            messageId=await ClientMessageId.get_next_id(),
            messageFlags=MHFlags.FIN.value,  # Multi-part=False
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

            if (mh_record["messageFlags"] & MHFlags.FIN.value) != 0:
                # We have received a FIN-bit, i.e., the last reponse has been
                # read.
                print("Last message read from this protocol")
                break

        # Get full data array
        mh_record = dict(
            protocol=9,  # DataArray
            messageType=2,  # GetDataArrays
            correlationId=0,  # Ignored
            messageId=await ClientMessageId.get_next_id(),
            messageFlags=MHFlags.FIN.value,  # Multi-part=False
        )
        gda_record = dict(
            dataArrays={
                # XXX: Note that we use the pathInResource (pathInHdfFile) from
                # the Grid2dRepresentation, but the uri is for
                # EpcExternalPartReference!
                key: dict(
                    uri=epc_uri,
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
                # Note that GetDataArraysResponse includes the dimensions as well.
                # This means that if we are getting the full array, we do not
                # need to query the metadata first.
                # However, it is probably a good idea to check the full size
                # before requesting the entire array.
                data = record["dataArrays"][sorted(pir)[0]]["data"]["item"]["values"]
            pprint.pprint(record)

            if (mh_record["messageFlags"] & MHFlags.FIN.value) != 0:
                # We have received a FIN-bit, i.e., the last reponse has been
                # read.
                print("Last message read from this protocol")
                break

        data = np.asarray(data).reshape(dimensions)  # .reshape(dimensions[::-1])
        print(data.shape)

        # Get first 5 rows of the data array
        mh_record = dict(
            protocol=9,  # DataArray
            messageType=3,  # GetDataSubrrays
            correlationId=0,  # Ignored
            messageId=await ClientMessageId.get_next_id(),
            messageFlags=MHFlags.FIN.value,  # Multi-part=False
        )
        gds_record = dict(
            dataSubarrays={
                key: dict(
                    uid=dict(
                        uri=epc_uri,
                        pathInResource=val,
                    ),
                    starts=[0, 0],
                    # XXX: The server crashes if the last axis of counts is too large
                    counts=[3, 2],
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
                ).reshape(record["dataSubarrays"][sorted(pir)[0]]["dimensions"])
            pprint.pprint(record)

            if (mh_record["messageFlags"] & MHFlags.FIN.value) != 0:
                # We have received a FIN-bit, i.e., the last reponse has been
                # read.
                print("Last message read from this protocol")
                break

        # Get the last 6 rows of the data array
        mh_record = dict(
            protocol=9,  # DataArray
            messageType=3,  # GetDataSubrrays
            correlationId=0,  # Ignored
            messageId=await ClientMessageId.get_next_id(),
            messageFlags=MHFlags.FIN.value,  # Multi-part=False
        )
        gds_record = dict(
            dataSubarrays={
                key: dict(
                    uid=dict(
                        uri=epc_uri,
                        pathInResource=val,
                    ),
                    starts=[0, 2],
                    counts=[3, 2],
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
                ).reshape(record["dataSubarrays"][sorted(pir)[0]]["dimensions"])
            pprint.pprint(record)

            if (mh_record["messageFlags"] & MHFlags.FIN.value) != 0:
                # We have received a FIN-bit, i.e., the last reponse has been
                # read.
                print("Last message read from this protocol")
                break

        subdata_rec = np.concatenate([subdata, subdata_2], axis=1)
        assert subdata_rec.shape == data.shape
        np.testing.assert_allclose(subdata_rec, data)

        await close_session(ws, "We are done 💅")


# TODO: Populate localhost with data
asyncio.run(start_and_stop())


# asyncio.run(
#     start_and_stop(
#         url="wss://interop-rddms.azure-api.net",
#         headers={"Authorization": "Bearer " + get_token()},
#     ),
# )
