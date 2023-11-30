import io
import pprint
import json
import fastavro


with open("etp.avpr", "r") as foo:
    jschema = json.load(foo)


def parse_func(js, named_schemas=dict()):
    ps = fastavro.schema.parse_schema(js, named_schemas)
    return fastavro.schema.fullname(ps), ps


etp_schemas = dict(parse_func(js) for js in jschema["types"])

pprint.pprint(sorted(etp_schemas))
pprint.pprint(etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"])
# pprint.pprint(etp_schemas["Energistics.Etp.v12.Datatypes.DataArrayTypes.DataArrayIdentifier"])
pprint.pprint(etp_schemas["Energistics.Etp.v12.Protocol.DataArray.GetDataArrays"])
pprint.pprint(etp_schemas["Energistics.Etp.v12.Datatypes.EndpointCapabilityKind"])


mh_records = [
    dict(protocol=0, messageType=1, correlationId=0, messageId=2, messageFlags=0),
    dict(protocol=1, messageType=1, correlationId=0, messageId=2, messageFlags=0),
    dict(protocol=0, messageType=1, correlationId=0, messageId=2, messageFlags=1),
    dict(protocol=2, messageType=1, correlationId=0, messageId=2, messageFlags=0),
]

fo = io.BytesIO()
fastavro.write.schemaless_writer(
    fo, etp_schemas["Energistics.Etp.v12.Datatypes.MessageHeader"], mh_records[1]
)
print(fo.getvalue())


fo = io.BytesIO()
fastavro.write.schemaless_writer(
    fo,
    etp_schemas["Energistics.Etp.v12.Datatypes.EndpointCapabilityKind"],
    "ActiveTimeoutPeriod",
)
print(fo.getvalue())
