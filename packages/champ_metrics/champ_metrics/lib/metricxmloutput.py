import xml.etree.ElementTree as ET
import xml.dom.minidom
import datetime
import copy
from .loghelper import Logger

DATECREATEDFIELD = "GenerationDate"
VERSIONFIELD = "ModelVersion"


def writeMetricsToXML(dMetricsArg, visitID, sourceDir, xmlFilePath, modelEngineRootNode, modelVersion):
    """
    Write a dictionary of metrics to an XML file.
    """

    log = Logger("Metric XML")
    log.info(f"Writing metrics to XML file: {xmlFilePath}")

    tree = ET.ElementTree(ET.Element(modelEngineRootNode))

    nodMeta = ET.SubElement(tree.getroot(), 'Meta')

    datetag = ET.SubElement(nodMeta, DATECREATEDFIELD)
    datetag.text = datetime.datetime.now().isoformat()

    version = ET.SubElement(nodMeta, VERSIONFIELD)
    version.text = modelVersion

    nodVisitID = ET.SubElement(nodMeta, 'VisitID')
    nodVisitID.text = str(visitID)

    nodMetrics = ET.SubElement(tree.getroot(), 'Metrics')
    _writeDictionaryToXML(nodMetrics, dMetricsArg)

    rough_string = ET.tostring(tree.getroot(), 'utf-8')
    reparsed = xml.dom.minidom.parseString(rough_string)
    pretty = reparsed.toprettyxml(indent="\t")
    with open(xmlFilePath, "w", encoding="utf-8") as f:
        f.write(pretty)

    log.info(f"Wrote metrics to file: {xmlFilePath}")


def _writeDictionaryToXML(parentNode, dValues):

    for xmlTag, item in sorted(dValues.items()):
        newParent = ET.SubElement(parentNode, xmlTag)
        if isinstance(item, dict):
            _writeDictionaryToXML(newParent, item)
        elif isinstance(item, list):
            for listItem in item:
                nodChild = ET.SubElement(newParent, 'Item')
                _writeDictionaryToXML(nodChild, listItem)
        elif item is not None:
            newParent.text = "{0}".format(item)


def integrateMetricDictionary(topo_metrics, prefix, newCollection):
    """
    Incorporate a dictionary into another one
    :param topo_metrics: The parent dictionary you want to fold thigns into
    :param prefix: The name of the new collection in the parent dictionary
    :param newCollection: The child collection you want to fold into the parent
    :return:
    """
    if not newCollection:
        return

    log = Logger("Integrating Metrics")
    log.info(f"Integrating metrics for {prefix}")

    topo_metrics[prefix] = {}
    for key, item in newCollection.items():
        topo_metrics[prefix][key] = copy.deepcopy(item)

    log.info(f"Finished integrating metrics for {prefix}")


def integrateMetricList(topo_metrics, parentPrefix, itemPrefix, newCollection):
    topo_metrics[parentPrefix] = []
    for item in newCollection:
        topo_metrics[parentPrefix].append(item)
