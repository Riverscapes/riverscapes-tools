from os import path
import sqlite3
import xml.etree.ElementTree as ET
import xml.dom.minidom
import json
import datetime
from rscommons import Logger
from champ_metrics.lib.exception import DataException

dUnitDefs = {
    'Fast-NonTurbulent/Glide': ['Fast-NonTurbulent/Glide'],
    'Fast-Turbulent': ['Riffle', 'Rapid', 'Cascade', 'Falls'],
    'Slow/Pool': ['Scour Pool', 'Plunge Pool', 'Dam Pool', 'Beaver Pool', 'Off Channel'],
    'Small Side Channel': ['Small Side Channel']
}


def getCleanTierName(sTierName):
    try:
        return sTierName.replace(' ', '').replace('/', '').replace('-', '')
    except Exception as e:
        raise DataException("Invalid or null tiername passed to 'getCleanTierName()'") from e


def createChannelUnitXMLFile(visitID, workbenchDB, xmlFilePath):
    """
    Loads a dictionary of channel unit information and writes it to XML file
    :param visitID: VisitID
    :param workbenchDB: Full path to the SQLite workbench database
    :param xmlFilePath: File path where the XML file will get created
    :return:
    """

    dUnits = loadChannelUnitsFromSQLite(visitID, workbenchDB)
    writeChannelUnitsToXML(visitID, dUnits, xmlFilePath)


def createChannelUnitJSONFile(visitID, workbenchDB, jsonFilePath):

    dUnits = loadChannelUnitsFromSQLite(visitID, workbenchDB)
    writeChannelUnitsToJSON(jsonFilePath, dUnits)


def loadChannelUnitsFromSQLite(visitID, workbenchDB):
    """
    Loads a dictionary of channel unit information from the workbench SQLite database
    :param visitID: VisitID
    :param workbenchDB: Full path to the SQLite workbench database
    :return: dictionary of channel unit info.
    """

    dUnits = {}

    conn = sqlite3.connect(workbenchDB)
    c = conn.cursor()
    c.execute("select ChannelUnitNumber, Tier1, Tier2, SegmentNumber from CHaMP_ChannelUnits WHERE (VisitID = ?)", [visitID])
    for unitRow in c.fetchall():
        dUnits[unitRow[0]] = (unitRow[1], unitRow[2], unitRow[3])

    log = Logger("Channel Units")
    log.info(f"{len(dUnits)} channel units loaded from SQLite workbench DB")
    return dUnits


def writeChannelUnitsToXML(visitID, dUnits, xmlFilePath):
    """
        Writes a dictionary of channel unit information to the specified XML path
        :param visitID: VisitID
        :param dUnits: Dictionary of channel unit info (see above method for structure)
        :param xmlFilePath: File path where the XML file will get created
        :return:
      """

    tree = ET.ElementTree(ET.Element('ChannelUnits'))

    nodMeta = ET.SubElement(tree.getroot(), 'Meta')
    datetag = ET.SubElement(nodMeta, 'DateCreated')
    datetag.text = datetime.datetime.now().isoformat()

    nodVisitID = ET.SubElement(nodMeta, 'VisitID')
    nodVisitID.text = str(visitID)

    nodUnits = ET.SubElement(tree.getroot(), 'Units')

    for cuNumber, unit in dUnits.items():
        nodUnit = ET.SubElement(nodUnits, 'Unit')

        nodNumber = ET.SubElement(nodUnit, "ChannelUnitNumber")
        nodNumber.text = str(cuNumber)

        nodTier1 = ET.SubElement(nodUnit, "Tier1")
        nodTier1.text = unit[0]

        nodTier2 = ET.SubElement(nodUnit, "Tier2")
        nodTier2.text = unit[1]

        nodSegment = ET.SubElement(nodUnit, "Segment")
        nodSegment.text = str(unit[2])

    rough_string = ET.tostring(tree.getroot(), 'utf-8')
    reparsed = xml.dom.minidom.parseString(rough_string)
    pretty = reparsed.toprettyxml(indent="\t")
    f = open(xmlFilePath, "wb")
    f.write(pretty)
    f.close()

# Sitka API no longer supported


def loadChannelUnitsFromAPI(vid):

    raise DataException("Loading channel units from the API is no longer supported.")

    # apiUnits = APIGet('visits/{}/measurements/Channel Unit'.format(vid))
    # dUnits = {}

    # for nodUnit in apiUnits['values']:
    #     value = nodUnit['value']
    #     nCUNumber = int(value['ChannelUnitNumber'])
    #     tier1 = value['Tier1']
    #     tier2 = value['Tier2']
    #     segment = value['ChannelSegmentID']

    #     dUnits[nCUNumber] = (tier1, tier2, segment)

    # log = Logger("Channel Units")
    # log.info("{0} channel units loaded from XML file".format(len(dUnits)))

    # return dUnits


def loadChannelUnitsFromJSON(jsonFilePath):
    if jsonFilePath is not None and not path.isfile(jsonFilePath):
        raise FileNotFoundError(f"Missing channel unit file at {jsonFilePath}")

    dUnits = {}

    with open(jsonFilePath) as data_file:
        data = json.load(data_file)

        for nodUnit in data['value']:
            value = nodUnit['value']
            nCUNumber = int(value['ChannelUnitNumber'])
            tier1 = value['Tier1']
            tier2 = value['Tier2']
            segment = value['ChannelSegmentID']

            dUnits[nCUNumber] = (tier1, tier2, segment)

    log = Logger("Channel Units")
    log.info(f"{len(dUnits)} channel units loaded from XML file")

    return dUnits


def writeChannelUnitsToJSON(jsonFilePath, dUnits):

    dJson = {}
    dJson["value"] = []

    for nChannelUnitNumber, aUnit in dUnits.items():
        value = {}
        value["ChannelUnitNumber"] = nChannelUnitNumber
        value["Tier1"] = aUnit[0]
        value["Tier2"] = aUnit[1]
        value["ChannelSegmentID"] = aUnit[2]

        outDict = {}
        outDict["value"] = value
        dJson["value"].append(outDict)

    with open(jsonFilePath, 'w', encoding='utf-8') as outfile:
        json.dump(dJson, outfile, indent=4)


def loadChannelUnitsFromXML(xmlFilePath):

    if not path.isfile(xmlFilePath):
        raise FileNotFoundError(f"Missing channel unit file at {xmlFilePath}")

    tree = ET.parse(xmlFilePath)
    nodRoot = tree.getroot()

    dUnits = {}
    for nodUnit in nodRoot.findall('Units/Unit'):
        nCUNumber = int(nodUnit.find('ChannelUnitNumber').text)
        tier1 = nodUnit.find('Tier1').text
        tier2 = nodUnit.find('Tier2').text
        segment = nodUnit.find('Segment').text

        dUnits[nCUNumber] = (tier1, tier2, segment)

    log = Logger("Channel Units")
    log.info(f"{len(dUnits)} channel units loaded from XML file")

    return dUnits
