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


from xsdata.models.datatype import XmlDateTime
from xsdata.formats.dataclass.context import XmlContext
from xsdata.formats.dataclass.parsers import XmlParser
from xsdata.formats.dataclass.serializers import XmlSerializer
from xsdata.formats.dataclass.serializers.config import SerializerConfig

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

async def delete_data_objects(ws, uris, pruneContainedObjects=False):
    mh_record = dict(
        protocol=4, # Store
        messageType=3, # DeleteDataObjects
        correlationId=0, # Ignored
        messageId=await ClientMessageId.get_next_id(),
        messageFlags=MHFlags.FIN.value, # Multi-part=False
    )

    ddo_record = dict(
        uris=dict((uri, uri) for uri in uris),
        # -- pruneContainedObjects:
        # let ETP server delete contained or related objects that are
        # not contained by any other data objects
        # Consider to set this to always be True when deleting
        # a map
        # FIXME: doesn't seem to be working correctly yet; consider filing 
        # an issue for ETP server
        pruneContainedObjects=pruneContainedObjects 
    )

    await ws.send(
        serialize_message(
            mh_record,
            ddo_record,
            "Energistics.Etp.v12.Protocol.Store.DeleteDataObjects"
        )
    )
    return await handle_multipart_response(
        ws, "Energistics.Etp.v12.Protocol.Store.DeleteDataObjectsResponse"
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

        # Start by listing all dataspaces on the ETP-server
        records = await get_dataspaces(ws, store_last_write_filter=None)

        # Delete the demo/rand-cli dataspace ('dataspace') if it exists (to
        # avoid piling up test-data)
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

        # Build the RESQML-objects "manually" from the generated dataclasses.
        # Their content is described also in the RESQML v2.0.1 standard that is
        # available for download here:
        # https://publications.opengroup.org/standards/energistics-standards/v231a
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

        # Generate some random offsets and rotation of the CRS.

        xoffset = np.random.random()
        yoffset = np.random.random()
        zoffset = np.random.random()
        rotation = np.random.random()

        # Example EPSG-code
        # https://epsg.io/9672
        vertical_epsg = 9672
        # Example EPSG-code
        # https://epsg.io/23029
        projected_epsg = 23029

        crs = resqml_objects.LocalDepth3DCrs(
            citation=resqml_objects.Citation(
                title="Random CRS",
                **common_citation_fields,
            ),
            schema_version=schema_version,
            uuid=str(uuid.uuid4()),
            xoffset=xoffset,
            yoffset=yoffset,
            zoffset=zoffset,
            areal_rotation=resqml_objects.PlaneAngleMeasure(
                value=rotation,
                uom=resqml_objects.PlaneAngleUom.DEGA,
            ),
            projected_axis_order=resqml_objects.AxisOrder2D.EASTING_NORTHING,
            projected_uom=resqml_objects.LengthUom.M,
            vertical_uom=resqml_objects.LengthUom.M,
            zincreasing_downward=True,
            vertical_crs=resqml_objects.VerticalCrsEpsgCode(
                epsg_code=vertical_epsg,
            ),
            projected_crs=resqml_objects.ProjectedCrsEpsgCode(
                epsg_code=projected_epsg,
            ),
        )

        # Create random test data
        # z_values = np.random.random((3, 4))

        # Create random test data with a fairly large array.  This one works
        # for the default websocket-sizes on the ETP-server (the one we use in
        # this example).
        z_values = np.random.random((2000, 1990))

        # TODO: Test with large array.
        # This requires subarray-handling.
        # The array below can not be sent or received in a single message.
        # z_values = np.random.random((2000, 2000))

        # When setting maximal websocket payload size (for the server, and
        # the client), the array below is the largest we can send and
        # recieve in a single message.
        # z_values = np.random.random((1, int(1.6e7 / 4) - 343))

        # Set some random origins, and calculate the step-size for each
        # direction
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
                                coordinate1=x0,
                                coordinate2=y0,
                                coordinate3=0.0,
                            ),
                            offset=[
                                # Offset for the y-direction, i.e., the fastest axis
                                resqml_objects.Point3DOffset(
                                    offset=resqml_objects.Point3D(
                                        coordinate1=0.0,
                                        coordinate2=1.0,
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
                                        coordinate1=1.0,
                                        coordinate2=0.0,
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

        # Set up the XML-serializer from xsdata in order to write the RESQML
        # dataclass objects above to XML.
        config = SerializerConfig(pretty_print=True)
        serializer = XmlSerializer(config=config)

        # NOTE: The name of the xsdata-generated dataclasses uses Python naming
        # convention (captical camel-case for classes), and the proper
        # data-object-type as recognized by RESQML and ETP is kept in the
        # internal Meta-class of the objects (if the name has changed).
        # This means that the data-object-type of a RESQML-object is _either_
        # just the class-name (if the name remains unchanged from the
        # xsdata-generation as in the EpcExternalPartReference-case), or it is
        # kept in <object>.Meta.name (as in the both the Grid2dRepresentation
        # and LocalDepth3dCrs cases).
        # A robust way of fetch the right data-object-type irrespective of where the name is kept is to use
        #
        #   data_object_type = getattr(<object>.Meta, "name", "") or <object>.__class__.__name__
        #
        # This fetches the name from <object>.Meta.name if that exists,
        # otherwise we use the the class-name (which will be the same as in the
        # RESQML-standard).
        get_data_object_type = (
            lambda obj: getattr(obj.Meta, "name", "") or obj.__class__.__name__
        )

        # Upload the EpcExternalPartReference-object to the ETP-server.
        records = await put_data_objects(
            ws,
            dataspace,
            [get_data_object_type(epc)],
            [epc.uuid],
            # Serialize the epc-object to XML
            [str.encode(serializer.render(epc))],
            titles=[epc.citation.title],
        )

        # Upload the LocalDepth3dCrs-object to the ETP-server
        records = await put_data_objects(
            ws,
            dataspace,
            [get_data_object_type(crs)],
            [crs.uuid],
            # Serialize the crs-object to XML
            [str.encode(serializer.render(crs))],
            titles=[crs.citation.title],
        )

        # Upload the Grid2dRepresentation-object to the ETP-server
        records = await put_data_objects(
            ws,
            dataspace,
            [get_data_object_type(gri)],
            [gri.uuid],
            # Serialize the gri-object to XML
            [str.encode(serializer.render(gri))],
            titles=[gri.citation.title],
        )

        # NOTE: All objects can be uploaded simultaneously by the call below:
        # records = await put_data_objects(
        #     ws,
        #     dataspace,
        #     [
        #         get_data_object_type(epc),
        #         get_data_object_type(crs),
        #         get_data_object_type(gri),
        #     ],
        #     [
        #         epc.uuid,
        #         crs.uuid,
        #         gri.uuid,
        #     ],
        #     [
        #         str.encode(serializer.render(epc)),
        #         str.encode(serializer.render(crs)),
        #         str.encode(serializer.render(gri)),
        #     ],
        #     titles=[
        #         epc.citation.title,
        #         crs.citation.title,
        #         gri.citation.title,
        #     ],
        # )

        # Upload the actual array data connected to the Grid2dRepresentation
        records = await put_data_arrays(
            ws,
            dataspace,
            # NOTE: This uses the data-object-type and uuid from the
            # EpcExternalPartReference-object.
            [get_data_object_type(epc)],
            [epc.uuid],
            # Fetch the key into the Hdf5-file (note that we do not use hdf5
            # locally, but this is the key that will be used on the server).
            [gri.grid2d_patch.geometry.points.zvalues.values.path_in_hdf_file],
            [z_values],
        )

        # Here we are done uploading all data to the ETP-server.
        # Next, we download the data again, and check that we have recovered
        # everything.

        # List all stored data on the ETP-server under 'dataspace'.
        records = await get_resources(ws, dataspace)

        # Fetch all uris (remember, we have deleted everything, so there should
        # only be three uris for the epc, crs and the gri-objects).
        uris = [
            [resource["uri"] for resource in record["resources"]] for record in records
        ]

        assert len(uris) == 1 and len(uris[0]) == 3
        # Verify that we are able to construct the same uris locally from
        # 'dataspace', the data-object-type and the corresponding uuid.
        assert (
            get_data_object_uri(dataspace, epc.__class__.__name__, epc.uuid) in uris[0]
        )
        assert get_data_object_uri(dataspace, crs.Meta.name, crs.uuid) in uris[0]
        assert get_data_object_uri(dataspace, gri.Meta.name, gri.uuid) in uris[0]

        # Download all three data objects from the ETP-server.
        records = await get_data_objects(ws, uris)
        # Check that all objects were downloaded in one go (these could be
        # split up into three different get_data_objects-calls, similarly to
        # the put_data_objects-calls above).
        assert len(records) == 1

        data_objects = records[0]["dataObjects"]
        keys = list(data_objects)

        # Find the relevant keys in the returned data.
        epc_key = next(filter(lambda x: "EpcExternalPartReference" in x, keys))
        crs_key = next(filter(lambda x: "LocalDepth3dCrs" in x, keys))
        gri_key = next(filter(lambda x: "Grid2dRepresentation" in x, keys))

        # Set up an XML-parser from xsdata.
        parser = XmlParser(context=XmlContext())

        # Construct the RESQML-dataclass objects from the raw binary XML-data
        # from the ETP-server.
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

        # Test that the returned objects are the same as the ones that were
        # uploaded. Note that this comparison are on the values inside the
        # objects, and not that they are the same type.
        assert epc_r == epc and id(epc_r) != id(epc)
        assert crs_r == crs and id(crs_r) != id(crs)
        assert gri_r == gri and id(gri_r) != id(gri)

        # Fetch relevant values from crs_r and gri_r and check that they are
        # the same as the ones that were inserted originally. The purpose here
        # is twofold: first, demonstrate how the values can be extracted from
        # the RESQML-dataclass, and second an extra test that the values are
        # indeed the same (sanity check for the comparison of the full objects
        # above).
        # NOTE: All tests use strict equality even though we are comparing
        # floating point numbers. The values should be preserved exactly, but
        # if this errors, perhaps some round-off do occur either on the server
        # or locally. An approximate equality might then be a more sensible
        # test.
        assert xoffset == crs_r.xoffset
        assert yoffset == crs_r.yoffset
        assert zoffset == crs_r.zoffset
        assert rotation == crs_r.areal_rotation.value
        assert vertical_epsg == crs_r.vertical_crs.epsg_code
        assert projected_epsg == crs_r.projected_crs.epsg_code

        assert z_values.shape[0] == gri_r.grid2d_patch.slowest_axis_count
        assert z_values.shape[1] == gri_r.grid2d_patch.fastest_axis_count
        assert (
            x0
            == gri_r.grid2d_patch.geometry.points.supporting_geometry.origin.coordinate1
        )
        assert (
            y0
            == gri_r.grid2d_patch.geometry.points.supporting_geometry.origin.coordinate2
        )
        assert (
            dx
            == gri_r.grid2d_patch.geometry.points.supporting_geometry.offset[
                1
            ].spacing.value
        )
        assert (
            dy
            == gri_r.grid2d_patch.geometry.points.supporting_geometry.offset[
                0
            ].spacing.value
        )

        # Construct the EpcExternalPartReference-uri from the returned
        # epc_r-object. We need this uri when we download the array data.
        epc_uri = get_data_object_uri(
            dataspace, resqml_objects.EpcExternalPartReference.__name__, epc_r.uuid
        )
        # Alternatively, fetch the epc-uri from the gri_r-object using the
        # pattern below.
        epc_uri_alt = get_data_object_uri(
            dataspace,
            re.search(
                r";type=(.+)",
                gri_r.grid2d_patch.geometry.points.zvalues.values.hdf_proxy.content_type,
            )[1],
            gri_r.grid2d_patch.geometry.points.zvalues.values.hdf_proxy.uuid,
        )
        # Check that both ways give the same uri
        assert epc_uri == epc_uri_alt

        # The metadata can be used to check the size of the array before
        # requesting the data. We could then optionally request the array in
        # several subarrays. If you know in advance that the full array can be
        # returned in one go, then this step is not necessary.
        # Here we use it to read out the shape of the array (which can be used
        # to determine the size).
        records = await get_data_array_metadata(
            ws,
            epc_uri,
            dict(
                grid=gri_r.grid2d_patch.geometry.points.zvalues.values.path_in_hdf_file
            ),
        )
        # Fetch shape of the array.
        z_shape = tuple(records[0]["arrayMetadata"]["grid"]["dimensions"])
        # Check that this is the same as the originally constructed array.
        assert z_shape == z_values.shape

        # Request the full array in one go.
        records = await get_data_arrays(
            ws,
            epc_uri,
            dict(
                grid=gri_r.grid2d_patch.geometry.points.zvalues.values.path_in_hdf_file
            ),
        )

        assert len(records) == 1
        data_arrays = records[0]["dataArrays"]

        # Convert the data to a NumPy-array. Considering that we only requested
        # a single array we could get rid of the dictionary wrapper here.
        data = {
            key: etp_data_array_to_numpy(records[0]["dataArrays"][key])
            for key in sorted(data_arrays)
        }

        # Check that the returned data is the same as the data we sent
        assert data[sorted(data)[0]].shape == z_shape
        np.testing.assert_allclose(
            data[sorted(data)[0]],
            z_values,
        )

        # Test the subarray capabilities. We request the full data array via
        # three subarray blocks, and reconstruct the full array in the end.
        # The shape of the blocks are:
        #
        #  [[sub_data_1, sub_data_2],
        #   [      sub_data_3      ]]
        #
        # where each sub_data_n is a block-array.
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
            counts=[2, z_values.shape[1] - 3],
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
            counts=[z_values.shape[0] - 2, z_values.shape[1]],
        )
        sub_data_3 = {
            key: etp_data_array_to_numpy(records[0]["dataSubarrays"][key])
            for key in sorted(records[0]["dataSubarrays"])
        }

        # Reconstruct the full array from the three blocks.
        subdata_rec = np.block(
            [
                [list(sub_data_1.values())[0], list(sub_data_2.values())[0]],
                [list(sub_data_3.values())[0]],
            ]
        )
        # Test that we have reconstructed the original data.
        np.testing.assert_allclose(z_values, subdata_rec)
        np.testing.assert_allclose(list(data.values())[0], subdata_rec)

        # Delete a 'map' object, i. e., delete all the resources of the dataspace 
        # given their uris
        records = await get_resources(ws, dataspace)
        uris = [resource["uri"] for resource in records[0]["resources"]]
        records = await delete_data_objects(ws, uris, False)
            
        # Test that all resources have been deleted
        records = await get_resources(ws, dataspace)
        assert len(records[0]["resources"]) == 0

        # Terminate the ETP-session, and return
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
