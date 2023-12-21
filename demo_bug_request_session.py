import io
import uuid
import time
import asyncio
import json
import datetime
import pprint
import enum

# Third-party imports
import websockets
import fastavro


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
        print(len(response))

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
        records = await request_session(ws, max_payload_size=int(1000))

        await close_session(ws, "We are done 💅")


# Run the roundtrip on localhost. This requires that the etp-server is running
# locally.
asyncio.run(start_and_stop())
