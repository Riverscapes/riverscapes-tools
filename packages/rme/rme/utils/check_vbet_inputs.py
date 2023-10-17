import os
from xml.etree import ElementTree as ET

from rscommons import Logger

from typing import List


def vbet_inputs(vbet_path: str, proj_paths: List):

    log = Logger('VBET Inputs')
    log.info('Checking that each input project has the same VBET input')

    vbet_guids = []

    vbxml = os.path.join(vbet_path, 'project.rs.xml')
    vbtree = ET.parse(vbxml)
    vbroot = vbtree.getroot()
    warehouse = vbroot.find('Warehouse')
    if warehouse is not None:
        vbet_guids.append(warehouse.attrib['id'])
    else:
        return None

    for path in proj_paths:
        projxml = os.path.join(path, 'project.rs.xml')
        tree = ET.parse(projxml)
        root = tree.getroot()
        meta = root.find('MetaData')
        for m in meta:
            if m.attrib['name'] == 'VBET Input':
                vb_guid = m.text[-36:]
                if vb_guid not in vbet_guids:
                    vbet_guids.append(vb_guid)

    if len(vbet_guids) == 0:
        log.info('No VBET inputs found')
        return None
    elif len(vbet_guids) == 1:
        log.info('Single VBET input found')
        return True
    else:
        log.info('Multiple VBET inputs found')
        return False
