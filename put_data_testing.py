import re
import os
import asyncio
import pprint

import websockets
import lxml.etree as ET

import talk_to_open_etp_server as etp_client


def get_path_in_resource(xml):
    if type(xml) in [str, bytes]:
        xml = ET.fromstring(xml)

    return xml.xpath("//*[starts-with(local-name(), 'PathInHdfFile')]")


def get_object_type_and_uuid(filename):
    uuid_pattern = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    obj_pattern = r"obj_[a-zA-Z0-9]+"
    pattern = r"(" + obj_pattern + r")_(" + uuid_pattern + r")"
    m = re.search(pattern, filename)

    return m.group(1), m.group(2)


async def start_and_stop():
    url = "ws://localhost:9002"
    headers = {}
    dataspace = "demo/rand-cli-manual"
    verify = True

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
        records = await etp_client.request_session(
            ws, application_name="puttycli", max_payload_size=int(1.6e7)
        )
        # Note, this size is the minimum of the suggested size sent in the
        # "RequestSession" and the returned value from the server in
        # "OpenSession".
        max_payload_size = records[0]["endpointCapabilities"][
            "MaxWebSocketMessagePayloadSize"
        ]["item"]

        records = await etp_client.get_dataspaces(ws, store_last_write_filter=None)

        if any(
            ds["path"] == dataspace for record in records for ds in record["dataspaces"]
        ):
            print(f"Deleting dataspace {dataspace}")
            records = await etp_client.delete_dataspaces(ws, [dataspace])

            if verify:
                # Test to see that the dataspace really was deleted
                records = await etp_client.get_dataspaces(
                    ws, store_last_write_filter=None
                )
                assert not any(
                    ds["path"] == dataspace
                    for record in records
                    for ds in record["dataspaces"]
                )

        # Now 'dataspace' does not exist
        # Next, we create it
        records = await etp_client.put_dataspaces(ws, [dataspace])
        if verify:
            # Test to see that the dataspace now exists
            records = await etp_client.get_dataspaces(ws, store_last_write_filter=None)
            assert any(
                ds["path"] == dataspace
                for record in records
                for ds in record["dataspaces"]
            )
        # Here 'dataspace' exists

        res_dat = {}
        for fname in filter(
            lambda x: x.startswith("res-") and x.endswith(".xml"), os.listdir(".")
        ):
            dot, _uuid = get_object_type_and_uuid(fname)
            with open(fname, "r") as f:
                res_dat[fname[len("res-") :]] = dict(
                    data_object_type=dot,
                    uuid=_uuid,
                    xml=f.read(),
                )

        w_dat = {}
        for fname in filter(
            lambda x: x.startswith("dat2-") and x.endswith(".xml"), os.listdir(".")
        ):
            dot, _uuid = get_object_type_and_uuid(fname)
            with open(fname, "r") as f:
                w_dat[fname[len("dat2-") :]] = dict(
                    data_object_type=dot,
                    uuid=_uuid,
                    xml=f.read(),
                )

        ext_key = next(filter(lambda x: "EpcExternal" in x, list(res_dat)))
        crs_key = next(filter(lambda x: "LocalDepth" in x, list(res_dat)))
        grd_key = next(filter(lambda x: "Grid2d" in x, list(res_dat)))

        dat = res_dat

        ext_dat = dat[ext_key]

        print(f"Uploading {ext_key}")
        records = await etp_client.put_data_objects(
            ws,
            dataspace,
            [ext_dat["data_object_type"]],
            [ext_dat["uuid"]],
            [str.encode(ext_dat["xml"])],
        )
        pprint.pprint(records)

        crs_dat = dat[crs_key]
        frk_crs = crs_dat["xml"]

        in_crs = w_dat[crs_key]["xml"].split("\n")

        fxml = frk_crs.split("\n")
        print("\n" + "\n".join(fxml) + "\n")
        print("\n" + "\n".join(in_crs) + "\n")

        # This is not enough. I suspect the first and last lines are relevant
        # fxml[1:-1] = [re.sub(r"resqml2", r"ns0", i) for i in in_crs[1:-2]]

        # This works, i.e., renaming the namespace does not alter the results (good)
        # fxml = [re.sub(r"resqml2", r"ns0", i) for i in in_crs]
        # fxml = [re.sub(r"eml", r"ns0", i) for i in fxml]

        print(fxml[0])
        # This also works! It seems the problem is the xsi:type-parameter in
        # the first line. I think maybe the XML parser used by open-etp-server
        # assumes that the namespace and the attribute is always separated by a
        # colon (':'). In the XML from lxml the namespace for the type is
        # included in the name by {url-to-namespace}obj_LocalDepth3dCrs, and
        # that seems to make it invalid for whatever parsing tool
        # open-etp-server uses.
        fxml[0] = re.sub(r"resqml2", r"ns0", in_crs[0])
        print(fxml[0])

        print("\n".join(fxml))

        print(f"Uploading {crs_key}")
        records = await etp_client.put_data_objects(
            ws,
            dataspace,
            [crs_dat["data_object_type"]],
            [crs_dat["uuid"]],
            [str.encode("\n".join(fxml))],
        )
        pprint.pprint(records)

        grd_dat = dat[grd_key]

        print(f"Uploading {grd_key}")
        records = await etp_client.put_data_objects(
            ws,
            dataspace,
            [grd_dat["data_object_type"]],
            [grd_dat["uuid"]],
            [str.encode(grd_dat["xml"])],
        )
        pprint.pprint(records)

        await etp_client.close_session(ws, reason="Mehmeh")


if __name__ == "__main__":
    asyncio.run(start_and_stop())
