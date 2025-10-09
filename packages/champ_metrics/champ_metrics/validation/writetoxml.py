import xml.etree.ElementTree as ET
import xml.dom.minidom
import datetime

from champmetrics.lib.metricxmloutput import DATECREATEDFIELD, VERSIONFIELD

def writeMetricsToXML(dataObj, dataStats, xmlFilePath):
    import validation
    tree = ET.ElementTree(ET.Element('TopoValidation'))

    nodMeta = ET.SubElement(tree.getroot(), 'Meta')

    datetag = ET.SubElement(nodMeta, DATECREATEDFIELD)
    datetag.text = datetime.datetime.now().isoformat()

    nodVer = ET.SubElement(nodMeta, VERSIONFIELD)
    nodVer.text = validation.__version__

    nodStatus = ET.SubElement(tree.getroot(), 'Status')

    nodError = ET.SubElement(nodStatus, 'Overall')
    nodError.text = str(dataStats["status"])

    nodError = ET.SubElement(nodStatus, 'Errors')
    nodError.text = str(dataStats["errors"])

    nodWarning = ET.SubElement(nodStatus, 'Warnings')
    nodWarning.text = str(dataStats["warnings"])

    nodNotTested = ET.SubElement(nodStatus, 'NotTested')
    nodNotTested.text = str(dataStats["nottested"])

    nodStatusLayers = ET.SubElement(nodStatus, 'Layers')
    if "layers" in dataStats:
        for layername, value in dataStats['layers'].items():
            nodNotTested = ET.SubElement(nodStatusLayers, layername)
            nodNotTested.text = str(value)


    nodLayers = ET.SubElement(tree.getroot(), 'Layers')
    for name,layer in dataObj.items():
        nodLayer = ET.SubElement(nodLayers, 'Layer')
        nodName = ET.SubElement(nodLayer, 'Name')
        nodName.text = name
        nodTests = ET.SubElement(nodLayer, 'Tests')
        for test in layer:
            nodTest = ET.SubElement(nodTests, 'Test')
            writeDictionaryToXML(nodTest, test)

    rough_string = ET.tostring(tree.getroot(), 'utf-8')
    reparsed = xml.dom.minidom.parseString(rough_string)
    pretty = reparsed.toprettyxml(indent="\t")
    f = open(xmlFilePath, "wb")
    f.write(pretty)
    f.close()

def writeDictionaryToXML(parentNode, dValues):

    for xmlTag, item in dValues.items():
        newParent = ET.SubElement(parentNode, xmlTag)
        if isinstance(item, dict):
            writeDictionaryToXML(newParent, item)
        elif isinstance(item, list):
            for listItem in item:
                nodChild = ET.SubElement(newParent, 'Item')
                writeDictionaryToXML(nodChild, listItem)
        else:
            newParent.text = "{0}".format(item)