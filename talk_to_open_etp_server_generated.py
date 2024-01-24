import re
import os
import sys
import io
import uuid
import time
import asyncio
import json
import datetime
import tempfile
import warnings
import pprint
import enum
import zipfile

# Third-party imports
import lxml.etree as ET
import httpx
import websockets
import fastavro
import numpy as np
import resqpy
import resqpy.model


from xsdata.models.datatype import XmlDateTime
from xsdata.formats.dataclass.context import XmlContext
from xsdata.formats.dataclass.parsers import XmlParser
from xsdata.formats.dataclass.serializers import XmlSerializer
from xsdata.formats.dataclass.serializers.config import SerializerConfig
from xsdata.formats.dataclass.models.generics import DerivedElement

import resqml_objects


def get_token(token_filename: str = ".token") -> str:
    with open(".token", "r") as f:
        f_r = json.load(f)
        token = f_r.get("accessToken")
        expiration = datetime.datetime.fromtimestamp(f_r.get("expires_on"))

        if expiration < datetime.datetime.now():
            warnings.warn("Token has expired")

        return token


def get_azurehost_capabilities(token: str):
    # Note that this get-request seems to be blocked from the hosted server.
    # However, it is not super important as the server responds with its
    # limitations in the OpenSession protocol.
    url = "https://interop-rddms.azure-api.net/.well-known/etp-server-capabilities?GetVersion=etp12.energistics.org"
    headers = {"Authorization": "Bearer " + token}
    return httpx.get(url, headers=headers).raise_for_status().json()


# token = get_token()
# pprint.pprint(get_azurehost_capabilities(token))


def get_localhost_capabilities():
    url = "http://localhost:9002/.well-known/etp-server-capabilities?GetVersion=etp12.energistics.org"
    return httpx.get(url).raise_for_status().json()


# pprint.pprint(get_localhost_capabilities())


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
    # TODO: Possibly support compression?
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
    # Max size of a websocket message payload. Note that this value will most
    # likely be updated from the server. The user should check the returned
    # value instead.
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
            # TODO: Handle this better. We should not need to terminate the
            # session and the program. Most exceptions are not fatal.
            # Output error object
            pprint.pprint(record)
            # Close the session
            await close_session(ws, reason=f"Error from protocol '{schema_key}'")
            # Exit with error
            sys.exit(1)
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
    # Note, the uris contain the dataspace name, the data object type, and the
    # uuid. An alternative to passing the complete uris would be to pass in
    # each part separately. I am unsure what is easiest down the line.
    # Note also that we use the uri as the name of the data object to ensure
    # uniqueness.

    # Get data objects
    # If the number of uris is larger than the websocket size, we must send
    # multiple GetDataObjects-requests. The returned data objects can also be
    # too large, and would then require chunking.
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


async def get_data_array_metadata(ws, epc_uri, path_in_resources):
    # Get data array metadata
    mh_record = dict(
        protocol=9,  # DataArray
        messageType=6,  # GetDataArrayMetadata
        correlationId=0,  # Ignored
        messageId=await ClientMessageId.get_next_id(),
        messageFlags=MHFlags.FIN.value,  # Multi-part=False
    )
    gdam_record = dict(
        dataArrays={
            # Note that we use the pathInResource (pathInHdfFile) from the
            # Grid2dRepresentation, but the uri is for
            # EpcExternalPartReference!
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
            gdam_record,
            "Energistics.Etp.v12.Protocol.DataArray.GetDataArrayMetadata",
        )
    )

    return await handle_multipart_response(
        ws,
        "Energistics.Etp.v12.Protocol.DataArray.GetDataArrayMetadataResponse",
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
    # TODO: Figure out how too large arrays are handled.
    # Does this protocol respond with an error, and tell us that we need to use
    # GetDataSubarrays instead, or does it return multiple responses?

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
            # Note that we use the pathInResource (pathInHdfFile) from the
            # Grid2dRepresentation, but the uri is for
            # EpcExternalPartReference!
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
    url="ws://localhost:9002", headers={}, dataspace="demo/rand-cli", verify=True
):
    print(f"Talking to: {url}")
    # Note that websockets compress the messages by default, and further
    # compression with gzip might not be super effective.  However, this could
    # be useful to test.
    async with websockets.connect(
        url,
        extra_headers=headers,
        subprotocols=["etp12.energistics.org"],
        # This max_size parameter sets the maximum websocket frame size for
        # _incoming messages_.
        max_size=int(1.6e7),
    ) as ws:
        # XXX: Passing in a max_payload_size < 1000 (or thereabouts) will crash
        # the server!
        records = await request_session(ws, max_payload_size=int(1.6e7))
        # Note, this size is the minimum of the suggested size sent in the
        # "RequestSession" and the returned value from the server in
        # "OpenSession".
        max_payload_size = records[0]["endpointCapabilities"][
            "MaxWebSocketMessagePayloadSize"
        ]["item"]

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

        # Here 'dataspace' does not exist so we create it
        records = await put_dataspaces(ws, [dataspace])
        if verify:
            # Test to see that the dataspace now exists
            records = await get_dataspaces(ws, store_last_write_filter=None)
            assert any(
                ds["path"] == dataspace
                for record in records
                for ds in record["dataspaces"]
            )
        # Here 'dataspace' exists

        common_citation_fields = dict(
            creation=XmlDateTime.from_string(
                datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")
            ),
            originator="etp-testing",
            format="equinor:etp-testing",
        )
        schema_version = "2.0"

        epc = resqml_objects.EpcExternalPartReference(
            citation=resqml_objects.Citation(
                title="Hdf Proxy",
                **common_citation_fields,
            ),
            schema_version=schema_version,
            uuid=str(uuid.uuid4()),
            mime_type="application/x-hdf5",
        )

        crs = resqml_objects.LocalDepth3DCrs(
            citation=resqml_objects.Citation(
                title="Random CRS",
                **common_citation_fields,
            ),
            schema_version=schema_version,
            uuid=str(uuid.uuid4()),
            xoffset=0.0,
            yoffset=0.0,
            zoffset=0.0,
            areal_rotation=resqml_objects.PlaneAngleMeasure(
                value=0.0,
                uom=resqml_objects.PlaneAngleUom.DEGA,
            ),
            projected_axis_order=resqml_objects.AxisOrder2D.EASTING_NORTHING,
            projected_uom=resqml_objects.LengthUom.M,
            vertical_uom=resqml_objects.LengthUom.M,
            zincreasing_downward=True,
            vertical_crs=resqml_objects.VerticalCrsEpsgCode(
                # https://epsg.io/9672
                epsg_code=9672,
            ),
            projected_crs=resqml_objects.ProjectedCrsEpsgCode(
                # https://epsg.io/23029
                epsg_code=23029,
            ),
        )

        # Create random test data
        z_values = np.random.random((3, 4))

        # TODO: Test with large array.
        # This requires subarray-handling.
        # The array below can not be sent or received in a single message.
        # z_values = np.random.random((2000, 2000))

        # When setting maximal websocket payload size (for the server, and
        # the client), the array below is the largest we can send and
        # recieve in a single message.
        # z_values = np.random.random((1, int(1.6e7 / 4) - 343))

        x0 = np.random.random() * 1e6
        y0 = np.random.random() * 1e6
        dx = x0 / z_values.shape[0]
        dy = y0 / z_values.shape[1]

        gri = resqml_objects.Grid2DRepresentation(
            uuid=(grid_uuid := str(uuid.uuid4())),
            schema_version=schema_version,
            surface_role=resqml_objects.SurfaceRole.MAP,
            citation=resqml_objects.Citation(
                title="Random z-values",
                **common_citation_fields,
            ),
            grid2d_patch=resqml_objects.Grid2DPatch(
                # TODO: Perhaps we can use this for tiling?
                patch_index=0,
                # NumPy-arrays are C-ordered, meaning that the last index is
                # the index that changes most rapidly. In this case this means
                # that the columns are the fastest changing axis.
                fastest_axis_count=z_values.shape[1],
                slowest_axis_count=z_values.shape[0],
                geometry=resqml_objects.PointGeometry(
                    local_crs=resqml_objects.DataObjectReference(
                        # NOTE: See Energistics Identifier Specification 4.0
                        # (it is downloaded alongside the RESQML v2.0.1
                        # standard) section 4.1 for an explanation on the
                        # format of content_type.
                        content_type=f"application/x-resqml+xml;version={schema_version};type={crs.Meta.name}",
                        title=crs.citation.title,
                        uuid=crs.uuid,
                    ),
                    points=resqml_objects.Point3DZvalueArray(
                        supporting_geometry=resqml_objects.Point3DLatticeArray(
                            origin=resqml_objects.Point3D(
                                coordinate1=y0,
                                coordinate2=x0,
                                coordinate3=0.0,
                            ),
                            offset=[
                                # Offset for the y-direction, i.e., the fastest axis
                                resqml_objects.Point3DOffset(
                                    offset=resqml_objects.Point3D(
                                        coordinate1=1.0,
                                        coordinate2=0.0,
                                        coordinate3=0.0,
                                    ),
                                    spacing=resqml_objects.DoubleConstantArray(
                                        value=dy,
                                        count=z_values.shape[1] - 1,
                                    ),
                                ),
                                # Offset for the x-direction, i.e., the slowest axis
                                resqml_objects.Point3DOffset(
                                    offset=resqml_objects.Point3D(
                                        coordinate1=0.0,
                                        coordinate2=1.0,
                                        coordinate3=0.0,
                                    ),
                                    spacing=resqml_objects.DoubleConstantArray(
                                        value=dx,
                                        count=z_values.shape[0] - 1,
                                    ),
                                ),
                            ],
                        ),
                        zvalues=resqml_objects.DoubleHdf5Array(
                            values=resqml_objects.Hdf5Dataset(
                                path_in_hdf_file=f"/RESQML/{grid_uuid}/zvalues",
                                hdf_proxy=resqml_objects.DataObjectReference(
                                    content_type=f"application/x-eml+xml;version={schema_version};type={epc.__class__.__name__}",
                                    title=epc.citation.title,
                                    uuid=epc.uuid,
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )

        config = SerializerConfig(pretty_print=True)
        serializer = XmlSerializer(config=config)

        records = await put_data_objects(
            ws,
            dataspace,
            [epc.__class__.__name__],
            [epc.uuid],
            [str.encode(serializer.render(epc))],
            titles=[epc.citation.title],
        )

        records = await put_data_objects(
            ws,
            dataspace,
            [crs.Meta.name],
            [crs.uuid],
            [str.encode(serializer.render(crs))],
            titles=[crs.citation.title],
        )

        records = await put_data_objects(
            ws,
            dataspace,
            [gri.Meta.name],
            [gri.uuid],
            [str.encode(serializer.render(gri))],
            titles=[gri.citation.title],
        )

        records = await put_data_arrays(
            ws,
            dataspace,
            [getattr(epc.Meta, "name", "") or epc.__class__.__name__],
            [epc.uuid],
            [gri.grid2d_patch.geometry.points.zvalues.values.path_in_hdf_file],
            [z_values],
        )

        records = await get_resources(ws, dataspace)

        uris = [
            [resource["uri"] for resource in record["resources"]] for record in records
        ]

        assert len(uris) == 1
        assert (
            get_data_object_uri(dataspace, epc.__class__.__name__, epc.uuid) in uris[0]
        )
        assert get_data_object_uri(dataspace, crs.Meta.name, crs.uuid) in uris[0]
        assert get_data_object_uri(dataspace, gri.Meta.name, gri.uuid) in uris[0]

        records = await get_data_objects(ws, uris)
        assert len(records) == 1
        data_objects = records[0]["dataObjects"]
        keys = list(data_objects)

        epc_key = next(filter(lambda x: "EpcExternalPartReference" in x, keys))
        crs_key = next(filter(lambda x: "LocalDepth3dCrs" in x, keys))
        gri_key = next(filter(lambda x: "Grid2dRepresentation" in x, keys))

        parser = XmlParser(context=XmlContext())

        epc_r = parser.from_bytes(
            data_objects[epc_key]["data"],
            resqml_objects.EpcExternalPartReference,
        )
        crs_r = parser.from_bytes(
            data_objects[crs_key]["data"],
            resqml_objects.LocalDepth3DCrs,
        )
        gri_r = parser.from_bytes(
            data_objects[gri_key]["data"],
            resqml_objects.Grid2DRepresentation,
        )

        epc_uri = get_data_object_uri(
            dataspace, resqml_objects.EpcExternalPartReference.__name__, epc_r.uuid
        )

        # The metadata can be used to check the size of the array before
        # requesting the data. We should then optionally request the array in
        # several subarrays.
        records = await get_data_array_metadata(
            ws,
            epc_uri,
            dict(
                grid=gri_r.grid2d_patch.geometry.points.zvalues.values.path_in_hdf_file
            ),
        )

        # Request the full array.
        records = await get_data_arrays(
            ws,
            epc_uri,
            dict(
                grid=gri_r.grid2d_patch.geometry.points.zvalues.values.path_in_hdf_file
            ),
        )

        assert len(records) == 1
        data_arrays = records[0]["dataArrays"]

        data = {
            key: etp_data_array_to_numpy(records[0]["dataArrays"][key])
            for key in sorted(data_arrays)
        }

        # Check that the returned data is the same as the data we sent
        assert data[sorted(data)[0]].shape == z_values.shape
        np.testing.assert_allclose(
            data[sorted(data)[0]],
            z_values,
        )

        # Test the subarray capabilities. We request the full data array via
        # three subarray blocks, and reconstruct the full array in the end.
        records = await get_data_subarray(
            ws,
            epc_uri,
            gri_r.grid2d_patch.geometry.points.zvalues.values.path_in_hdf_file,
            starts=[0, 0],
            counts=[2, 3],
        )

        sub_data_1 = {
            key: etp_data_array_to_numpy(records[0]["dataSubarrays"][key])
            for key in sorted(records[0]["dataSubarrays"])
        }

        records = await get_data_subarray(
            ws,
            epc_uri,
            gri_r.grid2d_patch.geometry.points.zvalues.values.path_in_hdf_file,
            starts=[0, 3],
            counts=[2, 1],
        )
        sub_data_2 = {
            key: etp_data_array_to_numpy(records[0]["dataSubarrays"][key])
            for key in sorted(records[0]["dataSubarrays"])
        }

        records = await get_data_subarray(
            ws,
            epc_uri,
            gri_r.grid2d_patch.geometry.points.zvalues.values.path_in_hdf_file,
            starts=[2, 0],
            counts=[1, 4],
        )
        sub_data_3 = {
            key: etp_data_array_to_numpy(records[0]["dataSubarrays"][key])
            for key in sorted(records[0]["dataSubarrays"])
        }

        subdata_rec = np.block(
            [
                [list(sub_data_1.values())[0], list(sub_data_2.values())[0]],
                [list(sub_data_3.values())[0]],
            ]
        )
        np.testing.assert_allclose(list(data.values())[0], subdata_rec)

        await close_session(ws, "We are done 💅")


# Run the round-trip on localhost. This requires that the etp-server is running
# locally.
asyncio.run(start_and_stop())


# Test on the published etp-server. This requires a token for Azure, and access
# to the rddms-group. If the server crashes, please inform the rddms-group to
# have it restarted.
# asyncio.run(
#     start_and_stop(
#         url="wss://interop-rddms.azure-api.net",
#         headers={"Authorization": "Bearer " + get_token()},
#     ),
# )
