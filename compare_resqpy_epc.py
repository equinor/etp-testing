# This program is intended to compare the internal representation of
# RESQML-objects in resqpy, and the result it writes to an .epc-file and
# h5-array.

import os
import tempfile
import pprint
import zipfile

import numpy as np
import resqpy
import resqpy.model

import lxml.etree as ET


with tempfile.TemporaryDirectory() as tmpdirname:
    print(tmpdirname)
    # Create random test data
    model = resqpy.model.new_model(os.path.join(tmpdirname, "test.epc"), quiet=False)
    # This needs to be done in order to get a uuid
    crs = resqpy.crs.Crs(model, title="random-test-crs")
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
        title="random-test-data",
        originator="someon",
    )
    mesh.create_xml()
    mesh.write_hdf5()

    model.store_epc()

    # print(model.rels_forest.items())
    # print(model.other_forest.items())
    # print(ET.tostring(model.main_tree, pretty_print=True, encoding=str))

    dat = {}
    with zipfile.ZipFile(model.epc_file, "r") as zfile:
        for zinfo in zfile.infolist():
            if zinfo.filename.startswith("obj_"):
                with zfile.open(zinfo.filename) as f:
                    dat[zinfo.filename] = f.read()


print(list(dat))
print(list(model.parts_forest))

for key in list(model.parts_forest):
    em = model.parts_forest[key][2]
    ed = ET.fromstring(dat[key])
    ET.indent(em)
    print(ET.tostring(em, pretty_print=True, encoding=str))
    print(ET.tostring(ed, pretty_print=True, encoding=str))
