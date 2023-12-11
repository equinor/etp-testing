import pprint
import re
import uuid
import zipfile

from lxml import etree
from lxml.etree import ElementTree, fromstring, Element, XPath

from etptypes.energistics.etp.v12.datatypes.object.resource import Resource
from etptypes.energistics.etp.v12.datatypes.object.active_status_kind import (
    ActiveStatusKind,
)
from etptypes.energistics.etp.v12.datatypes.object.data_object import (
    DataObject,
)

from etptypes.energistics.etp.v12.protocol.store.put_data_objects import (
    PutDataObjects,
)


UUID_REGEX = (
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)

ENERGYML_NAMESPACES = {
    "eml": "http://www.energistics.org/energyml/data/commonv2",
    "prodml": "http://www.energistics.org/energyml/data/prodmlv2",
    "witsml": "http://www.energistics.org/energyml/data/witsmlv2",
    "resqml": "http://www.energistics.org/energyml/data/resqmlv2",
}


def extractResqmlUuid(content: str):
    return findUuid(content)


XML_TYPE_REGXP = r"<([\w]+:)?([\w]+)"


def extractResqmlURI(content: str, dataspace_name: str = None):
    pattern = re.compile(XML_TYPE_REGXP)
    # print("PATT ", pattern)
    result = pattern.search(content)
    # print("result ", result)
    return (
        "eml:///"
        + ("dataspace('" + dataspace_name + "')/" if dataspace_name is not None else "")
        + "resqml20."
        + result.group(2)
        + "("
        + extractResqmlUuid(content)
        + ")"
    )


def energyml_xpath(tree: Element, xpath: str):
    """A xpath research that knows energyml namespaces"""
    try:
        return XPath(xpath, namespaces=ENERGYML_NAMESPACES)(tree)
    except TypeError:
        return None


def findUuid(input: str):
    p = re.compile(UUID_REGEX)
    result = p.search(input)
    if result is not None:
        return result.group() if result else None
    else:
        return None


def find_uuid_in_xml(xml_content: bytes) -> str:
    try:
        tree = ElementTree(fromstring(xml_content))
        root = tree.getroot()
        return find_uuid_in_elt(root)
    except etree.XMLSyntaxError:
        print("Error reading xml")
    return None


def find_uuid_in_elt(root: Element) -> str:
    _uuids = energyml_xpath(root, "@uuid")
    if len(_uuids) <= 0:
        _uuids = energyml_xpath(root, "@UUID")
    return _uuids[0] if len(_uuids) > 0 else None


def put_data_object_by_path(path: str, dataspace_name: str = None):
    result = []
    # try:
    if path.endswith(".xml"):
        f = open(path)
        f_content = f.read()

        result.append(put_data_object(f_content, dataspace_name))
        f.close()
    elif path.endswith(".epc"):
        do_lst = {}
        try:
            with zipfile.ZipFile(path, "r") as zfile:
                for zinfo in zfile.infolist():
                    if zinfo.filename.endswith(".xml"):
                        # print('%s (%s --> %s)' % (zinfo.filename, zinfo.file_size, zinfo.compress_size))
                        with zfile.open(zinfo.filename) as myfile:
                            file_content = myfile.read()
                            if (
                                findUuid(zinfo.filename) is not None
                                or find_uuid_in_xml(file_content) is not None
                            ):
                                do_lst[len(do_lst)] = _create_data_object(
                                    file_content.decode("utf-8"),
                                    dataspace_name,
                                )
                            else:
                                print(f"Ignoring file : {zinfo.filename}")
        except FileNotFoundError:
            print(f"File {path} not found")
        result.append(PutDataObjects(data_objects=do_lst))
    else:
        print("Unkown file type")
    # except Exception as e:
    #     print("Except : ", e)

    return result


def _create_data_object(f_content: str, dataspace_name: str = None):
    uri = extractResqmlURI(f_content, dataspace_name)
    print("Sending data object at uri ", uri)
    real_uuid = uuid.UUID(extractResqmlUuid(f_content)).hex
    ressource = Resource(
        uri=uri,
        name=uri,  # + ".xml",
        source_count=0,
        target_count=0,
        last_changed=0,
        store_last_write=0,
        store_created=0,
        active_status=ActiveStatusKind.INACTIVE,
        alternate_uris=[],
        custom_data=[],
    )
    return DataObject(blob_id=real_uuid, resource=ressource, data=f_content)


if __name__ == "__main__":
    for pdo in put_data_object_by_path("test.epc", "demo/rand-cli"):
        pprint.pprint(pdo.dict(by_alias=True))
