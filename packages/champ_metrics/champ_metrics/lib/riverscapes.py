"""Riverscapes Project"""

from os import path
from uuid import uuid4
import xml.etree.ElementTree as ET


# No arcpy or other non-standard dependencies please!

class Project(object):
    """Riverscapes Project Class
    Represent the inputs, realizations and outputs for a confinement project and read,
    write and update xml project file."""

    def __init__(self, xmlfile=None):

        self.name = ''
        self.projectType = ""
        self.projectVersion = ""
        self.projectPath = ''
        self.xmlname = 'project.rs.xml'
        self.riverscapes_program_version = "1.2"
        self.riverscapes_python_version = "1.0"

        self.ProjectMetadata = {}
        self.Realizations = {}
        self.InputDatasets = {}

        if xmlfile:
            self.loadProjectXML(xmlfile)

    def create(self, projectName, projectType, projectVersion="", projectPath=""):

        self.name = projectName
        self.projectType = projectType
        self.projectVersion = projectVersion
        self.projectPath = projectPath
        self.addProjectMetadata("RiverscapesProgramVersion", self.riverscapes_program_version)
        self.addProjectMetadata("RiverscapesPythonProjectVersion", self.riverscapes_python_version)

    def addProjectMetadata(self, Name, Value):
        self.ProjectMetadata[Name] = Value

    def addInputDataset(self, name, id, relativePath, sourcePath=None, datasettype="Vector", guid=None):

        newInputDataset = Dataset()
        newInputDataset.create(name, relativePath, datasettype, sourcePath, guid)

        # int = len(self.InputDatasets)
        newInputDataset.id = id
        self.InputDatasets[newInputDataset.name] = newInputDataset

        return newInputDataset

    def get_dataset_id(self, absfilename):

        relpath = path.relpath(absfilename, self.projectPath)
        id = ""

        for name, dataset in self.InputDatasets.items():
            if relpath == dataset.relpath:
                id = dataset.id

        return id

    def loadProjectXML(self, XMLpath):

        self.projectPath = path.dirname(XMLpath)
        self.xmlname = path.basename(XMLpath)

        # Open and Verify XML.
        tree = ET.parse(XMLpath)
        root = tree.getroot()
        # TODO Validate XML, If fail, throw validation error
        # TODO Project Type self.projectType == "Confinement"

        # Load Project Level Info
        self.name = root.find("Name").text
        self.projectType = root.find("ProjectType").text
        for meta in root.findall("./MetaData/Meta"):
            self.ProjectMetadata[meta.get("name")] = meta.text

        # Load Input Datasets
        for inputDatasetXML in root.findall("./Inputs/*"):  # Vector
            inputDataset = Dataset()
            inputDataset.createFromXMLElement(inputDatasetXML)
            self.InputDatasets[inputDataset.id] = inputDataset

        # Load Realizations
        # if self.projectType == "Topo":
        #     for realizationXML in root.findall("./Realizations/SurveyData/"):
        #         realization = SurveyDataRealization()
        #         realization.createFromXMLElement(realizationXML, self.InputDatasets)
        #         self.Realizations[realization.name] = realization
        #     for realizationXML in root.findall("./Realizations/SurveyData/"):
        #         realization = TopographyRealization()
        #         realization.createFromXMLElement(realizationXML, self.InputDatasets)
        #         self.Realizations[realization.name] = realization

        else:
            for realizationXML in root.findall("./Realizations/" + self.projectType):
                if self.projectType == "Confinement":
                    realization = ConfinementRealization()
                elif self.projectType == "GNAT":
                    realization = GNATRealization()
                else:
                    realization = Realization(self.projectType)
                realization.createFromXMLElement(realizationXML, self.InputDatasets)
                self.Realizations[realization.name] = realization


        # TODO Link Realization objects with inputs?

        return

    def get_next_realization_id(self):

        intRealizations = len(self.Realizations)
        realization_id = self.projectType + "_" + str(intRealizations + 1).zfill(3)

        return realization_id

    def addRealization(self, realization, realization_id=""):

        if realization_id:
            realization.id = realization_id
        else:
            intRealizations = len(self.Realizations)
            realization.id = self.projectType + "_" + str(intRealizations + 1).zfill(3)

        self.Realizations[realization.name] = realization
        return

    def writeProjectXML(self, XMLpath=None):

        if not XMLpath:
            XMLpath = path.join(self.projectPath, self.xmlname)

        rootProject = ET.Element("Project")
        rootProject.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        rootProject.set("xsi:noNamespaceSchemaLocation",
                        "https://raw.githubusercontent.com/Riverscapes/Program/master/Project/XSD/V1/Project.xsd")

        nodeName = ET.SubElement(rootProject, "Name")
        nodeName.text = self.name

        nodeType = ET.SubElement(rootProject, "ProjectType")
        nodeType.text = self.projectType

        nodeMetadata = ET.SubElement(rootProject, "MetaData")
        for metaName, metaValue in self.ProjectMetadata.items():
            nodeMeta = ET.SubElement(nodeMetadata, "Meta", {"name": metaName})
            nodeMeta.text = metaValue

        if len(self.InputDatasets) > 0:
            nodeInputDatasets = ET.SubElement(rootProject, "Inputs")
            for inputDatasetName, inputDataset in self.InputDatasets.items():
                nodeInputDataset = inputDataset.getXMLNode(nodeInputDatasets)

        if len(self.Realizations) > 0:
            nodeRealizations = ET.SubElement(rootProject, "Realizations")
            for realizationName, realizationInstance in self.Realizations.items():
                nodeRealization = realizationInstance.getXMLNode(nodeRealizations)

        indent(rootProject)
        tree = ET.ElementTree(rootProject)
        tree.write(XMLpath, 'utf-8', True)

        # TODO Add warning if xml does not validate??

        return


class Realization(object):
    def __init__(self, realization_type):
        self.promoted = False
        self.dateCreated = ''
        self.productVersion = ''
        self.guid = ''
        self.name = ''
        self.id = 'Not_Assigned'
        self.realization_type = realization_type

        self.parameters = {}
        self.metadata = {}
        self.analyses = {}
        self.inputs = {}
        self.outputs = {}

    def create(self, name):
        import datetime
        self.dateCreated = str(datetime.datetime.utcnow())
        self.productVersion = ''
        self.guid = str(uuid4())
        self.name = name

    def createFromXMLElement(self, xmlElement, dictInputDatasets):

        # Pull Realization Data
        self.promoted = xmlElement.get("promoted")
        self.dateCreated = xmlElement.get("dateCreated")
        self.productVersion = xmlElement.get('productVersion')
        self.id = xmlElement.get('id')
        self.guid = xmlElement.get('guid')

        self.name = xmlElement.find('Name').text

        # Pull Meta
        for nodeMeta in xmlElement.findall("./MetaData/Meta"):
            self.metadata[nodeMeta.get('name')] = nodeMeta.text

        # Pull Parameters
        for param in xmlElement.findall("./Parameters/Param"):
            self.parameters[param.get('name')] = param.text

        # Pull Analyses
        for analysisXML in xmlElement.findall("./Analyses/*"):
            analysis = Analysis()
            analysis.createFromXMLElement(analysisXML)
            self.analyses[analysis.name] = analysis

            # Pull Inputs

            # TODO Get inputs, possibly nested (need a way to store this for xml writing), could be ref or Dataset

        return

    def getXMLNode(self, xmlNode):

        # Prepare Attributes
        attributes = {}
        attributes['promoted'] = str(self.promoted).lower()
        attributes['dateCreated'] = str(self.dateCreated)
        attributes['id'] = str(self.id)
        if self.productVersion:
            attributes['productVersion'] = str(self.productVersion)
        if self.guid:
            attributes['guid'] = str(self.guid)

        nodeRealization = ET.SubElement(xmlNode, self.realization_type, attributes)
        nodeRealizationName = ET.SubElement(nodeRealization, "Name")
        nodeRealizationName.text = self.name

        # Add metadata to xml
        if self.metadata:
            nodeInputDatasetMetaData = ET.SubElement(nodeRealization, "MetaData")
            for metaName, metaValue in self.metadata.items():
                nodeInputDatasetMeta = ET.SubElement(nodeInputDatasetMetaData, "Meta", {"name": metaName})
                nodeInputDatasetMeta.text = metaValue

        # Add Params to XML
        if self.parameters:
            nodeParameters = ET.SubElement(nodeRealization, "Parameters")
            for paramName, paramValue in self.parameters.items():
                nodeParam = ET.SubElement(nodeParameters, "Param", {"name": paramName})
                nodeParam.text = paramValue

        if self.realization_type not in ["Confinement", "GNAT"]:
            #Inputs
            nodeInputs = ET.SubElement(nodeRealization, "Inputs")
            for inputName, inputValue in self.inputs.items():
                nodeInput = inputValue.getXMLNode(nodeInputs) if type(inputValue) is Dataset else ET.SubElement(nodeInputs, inputName, {"ref": str(inputValue)})
            # Outputs
            nodeOutputs = ET.SubElement(nodeRealization, "Outputs")
            for outputName, outputDataset in self.outputs.items():
                outputDataset.getXMLNode(nodeOutputs)
            # analyses
            nodeRealization = self.getAnalysesNode(nodeRealization)

        return nodeRealization

    def getAnalysesNode(self, nodeRealization):

        # Realization Analyses
        if self.analyses:
            nodeAnalyses = ET.SubElement(nodeRealization, "Analyses")
            for analysis in self.analyses.values():
                analysis.getXMLNode(nodeAnalyses)

        return nodeRealization


class GNATRealization(Realization):
    def __init__(self):
        Realization.__init__(self, "GNAT")

        self.RawStreamNetwork = ''
        self.RawNetworkTable = ""

        self.GNAT_StreamNetwork = Dataset()
        self.GNAT_NetworkTable = Dataset()

    def createGNATRealization(self, name, refRawStreamNetwork, outputGNAT_Network, refRawNetworkTable=None,
                              outputGNAT_Table=None):

        super(GNATRealization, self).create(name)

        self.RawStreamNetwork = refRawStreamNetwork
        self.GNAT_StreamNetwork = outputGNAT_Network

        if refRawNetworkTable:
            self.RawNetworkTable = refRawNetworkTable
        if outputGNAT_Table:
            self.GNAT_NetworkTable = outputGNAT_Table

        return

    def createFromXMLElement(self, xmlElement, dictInputDatasets):

        super(GNATRealization, self).createFromXMLElement(xmlElement, dictInputDatasets)

        # Pull Inputs
        self.RawStreamNetwork = xmlElement.find('./Inputs/RawStreamNetwork').get('ref')
        if xmlElement.find('./Inputs/RawNetworkTable'):
            self.RawNetworkTable = xmlElement.find('./Inputs/RawNetworkTable').get('ref')

        # Pull Outputs
        self.GNAT_StreamNetwork.createFromXMLElement(
            xmlElement.find("./Outputs/Vector"))  # Named type GNAT_StreamNetwork???
        if xmlElement.find("./Outputs/GNAT_NetworkTable"):
            self.GNAT_NetworkTable.createFromXMLElement(xmlElement.find("./Outputs/GNAT_NetworkTable"))

        return

    def getXMLNode(self, xmlNode):

        # Create Node
        nodeRealization = super(GNATRealization, self).getXMLNode(xmlNode)

        # GNAT Realization Inputs
        nodeRealizationInputs = ET.SubElement(nodeRealization, "Inputs")
        nodeRawStreamNetwork = ET.SubElement(nodeRealizationInputs, "RawStreamNetwork")
        nodeRawStreamNetwork.set("ref", self.RawStreamNetwork)
        if self.RawNetworkTable:
            nodeRawNetworkTable = ET.SubElement(nodeRealizationInputs, "RawNetworkTable")
            nodeRawNetworkTable.set("ref", self.RawNetworkTable)

        # GNAT Realization Outputs
        nodeOutputs = ET.SubElement(nodeRealization, "Outputs")
        self.GNAT_StreamNetwork.getXMLNode(nodeOutputs)
        if self.GNAT_NetworkTable.name:
            self.GNAT_NetworkTable.getXMLNode(nodeOutputs)

        nodeRealization = self.getAnalysesNode(nodeRealization)

        return nodeRealization

    def newAnalysisNetworkSegmentation(self,
                                       analysisName,
                                       paramSegmentLength,
                                       paramFieldSegments,
                                       paramDownstreamID,
                                       paramStreamNameField,
                                       paramSegmentationMethod,
                                       paramBoolSplitAtConfluences,
                                       paramBoolRetainAttributes,
                                       outputSegmentedConfinement):

        analysis = Analysis()
        analysis.create(analysisName, "SegmentedNetworkAnalysis")

        analysis.parameters["SegmentField"] = paramFieldSegments
        analysis.parameters["SegmentLength"] = paramSegmentLength
        analysis.parameters["DownstreamID"] = paramDownstreamID
        analysis.parameters["StreamNameField"] = paramStreamNameField
        analysis.parameters["SegmentationMethod"] = paramSegmentationMethod
        analysis.parameters["SplitAtConfluences"] = paramBoolSplitAtConfluences
        analysis.parameters["RetainOriginalAttributes"] = paramBoolRetainAttributes
        analysis.outputDatasets["GNAT_SegmentedNetwork"] = outputSegmentedConfinement

        self.analyses[analysisName] = analysis

        return


class ConfinementRealization(Realization):
    def __init__(self):
        Realization.__init__(self, "Confinement")

        self.StreamNetwork = ''
        self.ValleyBottom = ''
        self.ChannelPolygon = ''

        self.OutputConfiningMargins = Dataset()
        self.OutputRawConfiningState = Dataset()

        self.analyses = {}

    def createConfinementRealization(self,
                                     name,
                                     refStreamNetwork,
                                     refValleyBottom,
                                     refChannelPolygon,
                                     OutputConfiningMargins,
                                     OutputRawConfiningState):
        """
        :param str name:
        :param str refStreamNetwork:
        :param str refValleyBottom:
        :param str refChannelPolygon:
        :param Dataset OutputConfiningMargins:
        :param Dataset OutputRawConfiningState:
        :return: none
        """

        super(ConfinementRealization, self).create(name)

        # StreamNetwork.type = "StreamNetwork"
        # ValleyBottom.type = "ValleyBottom"
        # ChannelPolygon.type = "ChannelPolygon"

        OutputConfiningMargins.type = "ConfiningMargins"
        OutputConfiningMargins.xmlSourceType = "ConfiningMargins"
        OutputRawConfiningState.type = "RawConfiningState"
        OutputRawConfiningState.xmlSourceType = "RawConfiningState"

        self.StreamNetwork = refStreamNetwork
        self.ValleyBottom = refValleyBottom
        self.ChannelPolygon = refChannelPolygon

        self.OutputConfiningMargins = OutputConfiningMargins
        self.OutputRawConfiningState = OutputRawConfiningState

    def createFromXMLElement(self, xmlElement, dictInputDatasets):
        super(ConfinementRealization, self).createFromXMLElement(xmlElement, dictInputDatasets)

        # Pull Inputs
        self.ValleyBottom = xmlElement.find('./Inputs/ValleyBottom').get('ref')
        self.ChannelPolygon = xmlElement.find('./Inputs/ChannelPolygon').get('ref')
        self.StreamNetwork = xmlElement.find('./Inputs/StreamNetwork').get('ref')

        # Pull Outputs
        self.OutputConfiningMargins.createFromXMLElement(xmlElement.find("./Outputs/ConfiningMargins"))
        self.OutputRawConfiningState.createFromXMLElement(xmlElement.find("./Outputs/RawConfiningState"))

        return

    def getXMLNode(self, xmlNode):
        # Create Node
        nodeRealization = super(ConfinementRealization, self).getXMLNode(xmlNode)

        # Confinement Realization Inputs
        nodeRealizationInputs = ET.SubElement(nodeRealization, "Inputs")
        nodeValleyBottom = ET.SubElement(nodeRealizationInputs, "ValleyBottom")
        nodeValleyBottom.set("ref", self.ValleyBottom)
        nodeChannelPolygon = ET.SubElement(nodeRealizationInputs, "ChannelPolygon")
        nodeChannelPolygon.set("ref", self.ChannelPolygon)
        nodeStreamNetwork = ET.SubElement(nodeRealizationInputs, "StreamNetwork")
        nodeStreamNetwork.set('ref', self.StreamNetwork)

        # Confinement Realization Outputs
        nodeOutputs = ET.SubElement(nodeRealization, "Outputs")
        self.OutputRawConfiningState.getXMLNode(nodeOutputs)
        self.OutputConfiningMargins.getXMLNode(nodeOutputs)

        nodeRealization = self.getAnalysesNode(nodeRealization)

        return nodeRealization

    def newAnalysisMovingWindow(self, analysisName, paramSeedPointDist, paramWindowSizes, outputSeedPoints, outputWindows, analysis_id=None):

        analysis = Analysis()
        analysis.create(analysisName, "MovingWindow")
        analysis.id = analysis_id

        analysis.parameters["SeedPointDistance"] = paramSeedPointDist
        analysis.parameters["WindowSizes"] = paramWindowSizes
        analysis.outputDatasets["SeedPointFile"] = outputSeedPoints
        analysis.outputDatasets["MovingWindowFile"] = outputWindows

        self.analyses[analysisName] = analysis

    def newAnalysisSegmentedNetwork(self, analysisName, paramFieldSegments, paramFieldConfinement, paramFieldConstriction, outputSegmentedConfinement, analysis_id=None):

        analysis = Analysis()
        analysis.create(analysisName, "ConfinementBySegments")
        analysis.id = analysis_id

        analysis.parameters["SegmentField"] = paramFieldSegments
        analysis.parameters["ConfinementField"] = paramFieldConfinement
        analysis.parameters["ConstrictionField"] = paramFieldConstriction
        analysis.outputDatasets["SegmentedConfinement"] = outputSegmentedConfinement

        self.analyses[analysisName] = analysis

        return


class VbetRealization(Realization):
    def __init__(self):
        Realization.__init__(self, "VBET")

        self.TopographyDEM = Dataset()
        self.TopographyFlow = Dataset()
        self.TopographySlope = Dataset()

        self.DrainageNetworkNetwork = Dataset()
        self.DrainageNetworkBuffers = {}

    def createFromXMLElement(self, xmlElement, dictInputDatasets):
        super(VbetRealization, self).createFromXMLElement(xmlElement, dictInputDatasets)

        # TODO: get the rest of the inputs, but could be ref or actual dataset object...


class SurveyDataRealization(Realization):
    def __init__(self, projection_status=None):
        Realization.__init__(self, "SurveyData")
        self.projection = projection_status
        self.datasets = {}
        self.survey_extents = {}

    def getXMLNode(self, xmlNode):
        nodeRealization = super(SurveyDataRealization, self).getXMLNode(xmlNode)
        nodeRealization.set("projected", str(self.projection).lower())

        if self.datasets:
            for dsName, dsValue in self.datasets.items():
                dsValue.getXMLNode(nodeRealization)

        if self.survey_extents:
            nodeSurveyExtents = ET.SubElement(nodeRealization, "SurveyExtents")
            for extentName, extent in self.survey_extents.items():
                extent.getXMLNode(nodeSurveyExtents)

        return nodeRealization


class TopographyRealization(Realization):
    def __init__(self, name, topotin, activetin=True):
        Realization.__init__(self, "Topography")
        self.create(name)
        self.promoted = True
        self.topotin = topotin
        self.activetin = activetin
        self.stages = {}
        self.topography = {}
        self.assocated_surfaces = {}

    def getXMLNode(self, xmlNode):
        nodeRealization = super(TopographyRealization, self).getXMLNode(xmlNode)

        self.topotin.attributes['active'] = str(self.activetin).lower()
        nodeTIN = self.topotin.getXMLNode(nodeRealization)

        if self.topography:
            for dsValue in self.topography.values():
                dsValue.getXMLNode(nodeTIN)

        if self.stages:
            nodeStage = ET.SubElement(nodeTIN, "Stages")
            for stage in self.stages.values():
                stage.getXMLNode(nodeStage)

        if self.assocated_surfaces:
            nodeAssoc = ET.SubElement(nodeTIN, "AssocSurfaces")
            for assoc in self.assocated_surfaces.values():
                assoc.getXMLNode(nodeAssoc)

        return nodeRealization


class Analysis(object):
    def __init__(self):
        self.name = ''
        self.type = ''
        self.id = ""
        self.parameters = {}
        self.outputDatasets = {}

    def create(self, analysis_name, analysis_type):
        self.name = analysis_name
        self.type = analysis_type
        return

    def createFromXMLElement(self, xmlElement):

        self.type = xmlElement.tag
        self.name = xmlElement.find("Name").text

        for param in xmlElement.findall("./Parameters/Param"):
            self.parameters[param.get('name')] = param.text

        for output in xmlElement.findall("./Outputs/*"):
            outputDS = Dataset()
            outputDS.createFromXMLElement(output)
            self.outputDatasets[outputDS.name] = outputDS

        return

    def addParameter(self, parameterName, parameterValue):
        self.parameters[parameterName] = parameterValue
        return

    def getXMLNode(self, xmlNode):

        nodeAnalysis = ET.SubElement(xmlNode, self.type)

        nodeAnalysisName = ET.SubElement(nodeAnalysis, 'Name')
        nodeAnalysisName.text = self.name

        nodeParameters = ET.SubElement(nodeAnalysis, "Parameters")
        for paramName, paramValue in self.parameters.items():
            nodeParam = ET.SubElement(nodeParameters, "Param", {"name": paramName})
            nodeParam.text = paramValue

        nodeOutputs = ET.SubElement(nodeAnalysis, "Outputs")
        for outputName, outputDataset in self.outputDatasets.items():
            outputDataset.getXMLNode(nodeOutputs)

        # TODO Writing Analysis Node

        return xmlNode


class Dataset(object):
    def __init__(self):
        self.id = 'NotAssinged'  # also ref
        self.name = ''
        self.guid = ''
        self.type = ''
        self.relpath = ''
        self.metadata = {}
        self.attributes = {}

    def create(self, name, relpath, type="Vector", origPath=None, guid=None):

        self.name = name  # also ref
        self.guid = guid if guid else str(uuid4())
        self.type = type
        self.relpath = relpath

        self.id = "NotAssigned"  # TODO make this unique!!!!

        if origPath:
            self.metadata["origPath"] = origPath

        return self

    def createFromXMLElement(self, xmlElement):

        self.id = xmlElement.get("id")
        self.guid = xmlElement.get('guid')
        self.name = xmlElement.find('Name').text
        self.relpath = xmlElement.find("Path").text

        self.type = xmlElement.tag

        for nodeMeta in xmlElement.findall("./MetaData/Meta"):
            self.metadata[nodeMeta.get('name')] = nodeMeta.text

        return

    def getXMLNode(self, xmlNode):

        # Prepare Attributes
        self.attributes["id"] = self.id
        if self.guid:
            self.attributes['guid'] = self.guid

        # Generate Node
        nodeInputDataset = ET.SubElement(xmlNode, self.type, self.attributes)

        nodeInputDatasetName = ET.SubElement(nodeInputDataset, "Name")
        nodeInputDatasetName.text = self.name
        if self.relpath:
            nodeInputDatasetPath = ET.SubElement(nodeInputDataset, "Path")
            nodeInputDatasetPath.text = self.relpath

        if self.metadata:
            nodeInputDatasetMetaData = ET.SubElement(nodeInputDataset, "MetaData")
            for metaName, metaValue in self.metadata.items():
                nodeInputDatasetMeta = ET.SubElement(nodeInputDatasetMetaData, "Meta", {"name": metaName})
                nodeInputDatasetMeta.text = metaValue

        return nodeInputDataset

    def absolutePath(self, projectPath):
        return path.join(projectPath, self.relpath)


def get_input_id(inpath, strInputName):
    import glob

    int_id = len(glob.glob(path.join(inpath, strInputName + "*"))) + 1

    return strInputName + str(int_id).zfill(3)


def indent(elem, level=0, more_sibs=False):
    """ Pretty Print XML Element
    Source: http://stackoverflow.com/questions/749796/pretty-printing-xml-in-python
    """

    i = "\n"
    if level:
        i += (level - 1) * '  '
    num_kids = len(elem)
    if num_kids:
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
            if level:
                elem.text += '  '
        count = 0
        for kid in elem:
            indent(kid, level + 1, count < num_kids - 1)
            count += 1
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
            if more_sibs:
                elem.tail += '  '
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i
            if more_sibs:
                elem.tail += '  '
