import os
import io
import uuid
import time
import asyncio
import json
import datetime
import tempfile
import pprint
import enum
import zipfile

# Third-party imports
import lxml.etree as ET
import websockets
import fastavro
import numpy as np
import resqpy
import resqpy.model


# Read and store ETP-schemas in a global dictionary
# The file etp.avpr can be downloaded from Energistics here:
# https://publications.opengroup.org/standards/energistics-standards/energistics-transfer-protocol/v234
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
    fo = io.BytesIO()
    fastavro.write.schemaless_writer(
        fo, etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"], header_record
    )
    fastavro.write.schemaless_writer(fo, etp_schemas[body_schema_key], body_record)

    return fo.getvalue()


def get_path_in_resource(xml):
    if type(xml) in [str, bytes]:
        xml = ET.fromstring(xml)

    return xml.xpath("//*[starts-with(local-name(), 'PathInHdfFile')]")


def get_title(xml):
    if type(xml) in [str, bytes]:
        xml = ET.fromstring(xml)

    # Assuming a single "Citation" tag, with a single "Title" tag
    return next(
        filter(
            lambda se: "Title" in se.tag,
            next(filter(lambda e: "Citation" in e.tag, xml.iter())).iter(),
        )
    ).text


def get_data_object_uri(dataspace, data_object_type, _uuid):
    if not data_object_type.startswith("resqml20") or not data_object_type.startswith(
        "eml20"
    ):
        data_object_type = (
            f"resqml20.{data_object_type}"
            if "EpcExternalPart" not in data_object_type
            else f"eml20.{data_object_type}"
        )

    return f"eml:///dataspace('{dataspace}')/{data_object_type}({_uuid})"


async def request_session(
    ws,
    max_payload_size=1e9,
    application_name="somecli",
    application_version="0.0.0",
    additional_supported_protocols=[],
    additional_supported_data_objects=[],
):
    # Request session
    mh_record = dict(
        protocol=0,  # Core protocol
        messageType=1,  # RequestSession
        correlationId=0,  # Ignored for RequestSession
        messageId=await ClientMessageId.get_next_id(),
        messageFlags=MHFlags.FIN.value,  # FIN-bit
    )

    rs_record = dict(
        applicationName=application_name,
        applicationVersion=application_version,
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
            *additional_supported_protocols,
        ],
        supportedDataObjects=[
            dict(  # SupportedDataObject
                qualifiedType="resqml20.*",
            ),
            dict(
                qualifiedType="eml20.*",
            ),
            *additional_supported_data_objects,
        ],
        currentDateTime=datetime.datetime.now(datetime.timezone.utc).timestamp(),
        earliestRetainedChangeTime=0,
        endpointCapabilities=dict(
            MaxWebSocketMessagePayloadSize=dict(
                item=max_payload_size,
            ),
        ),
    )

    await ws.send(
        serialize_message(
            mh_record,
            rs_record,
            "Energistics.Etp.v12.Protocol.Core.RequestSession",
        ),
    )

    # Note, OpenSession is a single message, but the
    # handle_multipart_response-function works just as well for single
    # messages.
    return await handle_multipart_response(
        ws, "Energistics.Etp.v12.Protocol.Core.OpenSession"
    )


async def close_session(ws, reason):
    # Close session
    mh_record = dict(
        protocol=0,  # Core
        messageType=5,  # CloseSession
        correlationId=0,  # Ignored
        messageId=await ClientMessageId.get_next_id(),
        messageFlags=MHFlags.FIN.value,
    )

    cs_record = dict(reason=reason)

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
        )

        if mh_record["messageType"] == 1000:
            # ProtocolException
            record = fastavro.read.schemaless_reader(
                fo,
                etp_schemas["Energistics.Etp.v12.Protocol.Core.ProtocolException"],
            )
            # Output error object
            pprint.pprint(record)
        else:
            record = fastavro.read.schemaless_reader(
                fo,
                etp_schemas[schema_key],
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
    gd_record = dict(storeLastWriteFilter=store_last_write_filter)

    await ws.send(
        serialize_message(
            mh_record, gd_record, "Energistics.Etp.v12.Protocol.Dataspace.GetDataspaces"
        )
    )

    return await handle_multipart_response(
        ws, "Energistics.Etp.v12.Protocol.Dataspace.GetDataspacesResponse"
    )


async def put_dataspaces(ws, dataspaces):
    uris = list(map(lambda dataspace: f"eml:///dataspace('{dataspace}')", dataspaces))

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
                    path=dataspace,
                    # Here we create the dataspace for the first time, hence last write
                    # and created are the same
                    storeLastWrite=time,
                    storeCreated=time,
                ),
            )
            for uri, dataspace in zip(uris, dataspaces)
        )
    )

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


async def put_data_objects(ws, dataspace, data_object_types, uuids, xmls, titles=None):
    # TODO: Use chunks if the data objects are too large for the websocket payload

    uris = [
        get_data_object_uri(dataspace, dot, _uuid)
        for dot, _uuid in zip(data_object_types, uuids)
    ]

    if not titles:
        titles = uris

    xmls = [xml if type(xml) in [str, bytes] else ET.tostring(xml) for xml in xmls]

    time = datetime.datetime.now(datetime.timezone.utc).timestamp()

    mh_record = dict(
        protocol=4,  # Store
        messageType=2,  # PutDataObjects
        correlationId=0,  # Ignored
        messageId=await ClientMessageId.get_next_id(),
        # This is in general a multi-part message, but we will here only send
        # one.
        messageFlags=MHFlags.FIN.value,
    )

    pdo_record = dict(
        dataObjects={
            title: dict(
                data=xml,
                format="xml",
                blobId=None,
                resource=dict(
                    uri=uri,
                    name=title,
                    lastChanged=time,
                    storeCreated=time,
                    storeLastWrite=time,
                    activeStatus="Inactive",
                ),
            )
            for title, uri, xml in zip(titles, uris, xmls)
        },
    )

    await ws.send(
        serialize_message(
            mh_record,
            pdo_record,
            "Energistics.Etp.v12.Protocol.Store.PutDataObjects",
        )
    )

    return await handle_multipart_response(
        ws,
        "Energistics.Etp.v12.Protocol.Store.PutDataObjectsResponse",
    )


async def get_resources(
    ws,
    dataspace,
    extra_context_info_args=dict(),
    extra_get_resources_args=dict(),
):
    uri = f"eml:///dataspace('{dataspace}')"

    # Get resources
    mh_record = dict(
        protocol=3,  # Store
        messageType=1,  # GetResources
        correlationId=0,  # Ignored
        messageId=await ClientMessageId.get_next_id(),
        messageFlags=MHFlags.FIN.value,
    )
    gr_record = {
        **dict(  # GetResources
            context={
                **dict(  # ContextInfo
                    uri=uri,
                    depth=2,
                    navigableEdges="Primary",
                ),
                **extra_context_info_args,
            },
            scope="targets",
            storeLastWriteFilter=None,
            activeStatusFilter=None,
        ),
        **extra_get_resources_args,
    }

    await ws.send(
        serialize_message(
            mh_record,
            gr_record,
            "Energistics.Etp.v12.Protocol.Discovery.GetResources",
        )
    )

    return await handle_multipart_response(
        ws,
        "Energistics.Etp.v12.Protocol.Discovery.GetResourcesResponse",
    )


async def get_data_objects(ws, uris):
    # Get data objects
    mh_record = dict(
        protocol=4,  # Store
        messageType=1,  # GetDataObjects
        correlationId=0,  # Ignored
        messageId=await ClientMessageId.get_next_id(),
        messageFlags=MHFlags.FIN.value,  # Multi-part=False
    )
    # Assuming that all uris fit in a single record, for now.
    gdo_record = dict(
        uris=dict((uri, uri) for _uris in uris for uri in _uris),
        format="xml",
    )

    await ws.send(
        serialize_message(
            mh_record,
            gdo_record,
            "Energistics.Etp.v12.Protocol.Store.GetDataObjects",
        )
    )

    return await handle_multipart_response(
        ws,
        "Energistics.Etp.v12.Protocol.Store.GetDataObjectsResponse",
    )


def numpy_to_etp_data_array(array):
    return dict(
        dimensions=list(array.shape),
        # See Energistics.Etp.v12.Datatypes.AnyArray for the "item"-key, and
        # Energistics.Etp.v12.Datatypes.ArrayOfDouble for the "values"-key.
        data=dict(item=dict(values=array.ravel().tolist())),
    )


def etp_data_array_to_numpy(data_array):
    return np.asarray(data_array["data"]["item"]["values"]).reshape(
        data_array["dimensions"]
    )


async def put_data_arrays(
    ws, dataspace, data_object_types, uuids, path_in_resources, arrays
):
    uris = [
        get_data_object_uri(dataspace, dot, _uuid)
        for dot, _uuid in zip(data_object_types, uuids)
    ]

    mh_record = dict(
        protocol=9,  # DataArray
        messageType=4,  # PutDataArrays
        correlationId=0,  # Ignored
        messageId=await ClientMessageId.get_next_id(),
        messageFlags=MHFlags.FIN.value,  # Multi-part=False
    )
    pda_record = dict(
        dataArrays={
            uri: dict(
                uid=dict(
                    uri=uri,
                    pathInResource=pir,
                ),
                array=numpy_to_etp_data_array(array),
            )
            for uri, pir, array in zip(uris, path_in_resources, arrays)
        }
    )

    await ws.send(
        serialize_message(
            mh_record,
            pda_record,
            "Energistics.Etp.v12.Protocol.DataArray.PutDataArrays",
        )
    )

    return await handle_multipart_response(
        ws,
        "Energistics.Etp.v12.Protocol.DataArray.PutDataArraysResponse",
    )


async def get_data_arrays(ws, epc_uri, path_in_resources):
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
            key: dict(
                uri=epc_uri,
                pathInResource=val,
            )
            for key, val in path_in_resources.items()
        },
    )

    await ws.send(
        serialize_message(
            mh_record,
            gda_record,
            "Energistics.Etp.v12.Protocol.DataArray.GetDataArrays",
        )
    )

    return await handle_multipart_response(
        ws,
        "Energistics.Etp.v12.Protocol.DataArray.GetDataArraysResponse",
    )


async def get_data_subarray(ws, epc_uri, path_in_resource, starts, counts, key=None):
    # This method only supports the request of a single subarray.
    # The protocol from ETP can support the request of multiple subarrays.

    mh_record = dict(
        protocol=9,  # DataArray
        messageType=3,  # GetDataSubarrays
        correlationId=0,  # Ignored
        messageId=await ClientMessageId.get_next_id(),
        messageFlags=MHFlags.FIN.value,  # Multi-part=False
    )
    gds_record = dict(
        dataSubarrays={
            key
            or "0": dict(
                uid=dict(
                    uri=epc_uri,
                    pathInResource=path_in_resource,
                ),
                starts=starts,
                # XXX: The server crashes if the last axis of counts is too large
                counts=counts,
            )
        },
    )

    await ws.send(
        serialize_message(
            mh_record,
            gds_record,
            "Energistics.Etp.v12.Protocol.DataArray.GetDataSubarrays",
        )
    )

    return await handle_multipart_response(
        ws, "Energistics.Etp.v12.Protocol.DataArray.GetDataSubarraysResponse"
    )


async def start_and_stop(
    url="ws://localhost:9002",
    headers={},
    dataspace="demo/rand-cli",
):
    print(f"Talking to: {url}")
    async with websockets.connect(
        url,
        extra_headers=headers,
        subprotocols=["etp12.energistics.org"],
        # This max_size parameter sets the maximum websocket frame size for
        # _incoming messages_. The value is the same as in open-etp-server.
        max_size=int(1.6e7),
    ) as ws:
        records = await request_session(ws, max_payload_size=int(1.6e7))
        # Note, the returned size is the minimum of the suggested size sent in
        # the "RequestSession" and the returned value from the server in
        # "OpenSession".
        max_payload_size = records[0]["endpointCapabilities"][
            "MaxWebSocketMessagePayloadSize"
        ]["item"]

        records = await get_dataspaces(ws, store_last_write_filter=None)

        # Delete the dataspace if it already exists
        if any(
            ds["path"] == dataspace for record in records for ds in record["dataspaces"]
        ):
            print(f"Deleting dataspace {dataspace}")
            records = await delete_dataspaces(ws, [dataspace])

        # Now 'dataspace' does not exist
        # Next, we create it
        records = await put_dataspaces(ws, [dataspace])

        with tempfile.TemporaryDirectory() as tmpdirname:
            # Note, resqpy does not seem to construct the correct xml-objects
            # before they are written to disk. As such, we have to write to
            # disk, then read in again to get the correct values. Here we do
            # that using a temporary directory to ensure that data on disk is
            # deleted after the context manager finishes.

            # Create random test data
            z_values = np.random.random((3, 4))

            # Set up model for test data
            model = resqpy.model.new_model(
                os.path.join(tmpdirname, "tmp.epc"), quiet=False
            )
            crs = resqpy.crs.Crs(model, title="random-test-crs")
            crs.create_xml()

            mesh = resqpy.surface.Mesh(
                model,
                crs_uuid=model.crs_uuid,
                mesh_flavour="reg&z",
                ni=z_values.shape[1],
                nj=z_values.shape[0],
                origin=(np.random.random(), np.random.random(), 0.0),
                dxyz_dij=np.array([[1.0, 0.0, 0.0], [0.0, 0.5, 0.0]]),
                z_values=z_values,
                title="random-test-data",
            )
            mesh.create_xml()
            # Write to disk (the hdf5-file is constructed already in the
            # model-constructor)
            mesh.write_hdf5()
            model.store_epc()

            # Read epc-file from disk
            dat = {}
            with zipfile.ZipFile(model.epc_file, "r") as zfile:
                for zinfo in filter(
                    lambda x: x.filename.startswith("obj_"), zfile.infolist()
                ):
                    with zfile.open(zinfo.filename) as f:
                        dat[zinfo.filename] = f.read()

        data_object_types = []
        uuids = []
        xmls = []
        titles = []
        path_in_resources = []
        for key, item in model.parts_forest.items():
            dot, _uuid, xml = item

            titles.append(get_title(xml))

            res = get_path_in_resource(xml)

            if len(res) == 1:
                path_in_resources.append(res[0].text)

            data_object_types.append(dot)
            uuids.append(_uuid)
            # Note that we here use the xmls from the etp-file instead of the
            # ones from the resqpy model.
            xmls.append(dat[key])

        # Upload the xml-objects
        records = await put_data_objects(
            ws,
            dataspace,
            data_object_types,
            uuids,
            xmls,
            # Titles are not unique
            titles=titles if len(titles) == len(set(titles)) else None,
        )

        epc_types, epc_uuids = zip(
            *filter(lambda x: "EpcExternal" in x[0], zip(data_object_types, uuids))
        )

        # Upload the actual data array
        records = await put_data_arrays(
            ws,
            dataspace,
            epc_types,
            epc_uuids,
            path_in_resources,
            [z_values],
        )

        # Start reading back from the server
        records = await get_resources(ws, dataspace)

        uris = [
            [resource["uri"] for resource in record["resources"]] for record in records
        ]

        # Read back xml-objects
        records = await get_data_objects(ws, uris)

        # Find the path to the array and store it in pir, and find the uri of
        # the EpcExternalPartReference-object
        pir = {}
        epc_uri = ""
        for record in records:
            for uri, items in record["dataObjects"].items():
                assert uri == items["resource"]["uri"]
                if "EpcExternalPart" in uri:
                    epc_uri = uri

                res = get_path_in_resource(items["data"])
                if len(res) == 1:
                    pir[uri] = res[0].text

        assert epc_uri != ""

        # XXX: There are two bugs connected to the GetDataSubarrays-protocol.
        # 1. It is is possible to define an arbitrary negative starting index
        # and read from there.
        # 2. By reading an invalid location in memory (using the method from 1.
        # above) we can crash the server.

        # Demonstration of 1. is given below. Here we set a negative starting
        # index and read until the end of the array.
        records = await get_data_subarray(
            ws,
            epc_uri,
            pir[sorted(pir)[0]],
            starts=[0, -4],
            # Size of array is (3, 4)
            counts=(c := [3, 8]),
            key=sorted(pir)[0],
        )
        arr = np.array(
            records[0]["dataSubarrays"][sorted(pir)[0]]["data"]["item"]["values"]
        ).reshape(c)
        # Here 'arr' contains the original array (with some duplicate values),
        # but also values outside of the array.

        # Demonstration of 2. is given below. Note, that the exact value can
        # vary, and I suspect it depends on size and layout of memory.
        records = await get_data_subarray(
            ws,
            epc_uri,
            pir[sorted(pir)[0]],
            starts=[0, -1000000000],
            # Size of array is (3, 4)
            counts=[1, 1],
            key=sorted(pir)[0],
        )

        await close_session(ws, "We are done 💅")


# Run the roundtrip on localhost. This requires that the etp-server is running
# locally.
asyncio.run(start_and_stop())
