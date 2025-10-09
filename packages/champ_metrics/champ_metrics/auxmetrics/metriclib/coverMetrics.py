from champmetrics.lib.loghelper import Logger
import numpy as np




def visitCoverMetrics(visitMetrics, visitobj):
    visit  = visitobj['visit']
    riparianStructures = visitobj['riparianStructures']

    percentBigTreeCover(visitMetrics, riparianStructures)
    percentCanopyNoCover(visitMetrics, riparianStructures)
    percentGroundCover(visitMetrics, riparianStructures)
    percentGroundCoverNoCover(visitMetrics, riparianStructures)
    percentUnderstoryCover(visitMetrics, riparianStructures)
    percentWoodyCover(visitMetrics, riparianStructures)
    percentNonWoodyGroundCover(visitMetrics, visit, riparianStructures)
    percentConiferousCover(visitMetrics, visit, riparianStructures)


def percentConiferousCover(visitMetrics, visit, riparianStructures):
    if visit["iterationID"] == 1:
        visitMetrics["PercentConiferousCover"] = getConiferousScore2011(riparianStructures)
    else:
        visitMetrics["PercentConiferousCover"] = getConiferousScore2012(riparianStructures)


def getConiferousScore2012(riparianStructures):
    if riparianStructures is None:
        return None

    inScope = []

    inScope.extend([s["value"]["LBCanopyWoodyConiferous"] + s["value"]["LBUnderstoryWoodyConiferous"] for s in riparianStructures["values"] if s["value"]["LBCanopyWoodyConiferous"] is not None and s["value"]["LBUnderstoryWoodyConiferous"] is not None])
    inScope.extend([s["value"]["RBCanopyWoodyConiferous"] + s["value"]["RBUnderstoryWoodyConiferous"] for s in riparianStructures["values"] if s["value"]["RBCanopyWoodyConiferous"] is not None and s["value"]["RBUnderstoryWoodyConiferous"] is not None])

    if inScope.__len__() > 0:
        return np.mean(inScope)
    else:
        return None


def getConiferousScore2011(riparianStructures):
    if riparianStructures is None:
        return None

    count = 0
    result = 0
    multiplicationFactors = {"Coniferous": 1, "Mixed": 0.5 }

    for rec in [r for r in riparianStructures["values"]]:
        if rec["value"]["LBCanopyBigTrees"] is not None and rec["value"]["LBCanopySmallTrees"] is not None and rec["value"]["LBCanopyVegetationType"] is not None:
            lbfactor = 0
            if rec["value"]["LBCanopyVegetationType"] in multiplicationFactors:
                lbfactor = multiplicationFactors[rec["value"]["LBCanopyVegetationType"]]

            lbunderstoryfactor = 0
            if  rec["value"]["LBUnderstoryVegetationType"] is not None and rec["value"]["LBUnderstoryVegetationType"] in multiplicationFactors:
                lbunderstoryfactor = multiplicationFactors[rec["value"]["LBUnderstoryVegetationType"]]

            result = result + (rec["value"]["LBCanopyBigTrees"] + rec["value"]["LBCanopySmallTrees"]) * lbfactor
            lbunderstoryshrubs = 0
            if rec["value"]["LBUnderstoryWoodyShrubs"] is not None:
                lbunderstoryshrubs = rec["value"]["LBUnderstoryWoodyShrubs"]
            result = result + (lbunderstoryshrubs * lbunderstoryfactor)
            count = count + 1

        if rec["value"]["RBCanopyBigTrees"] is not None and rec["value"]["RBCanopySmallTrees"] is not None and rec["value"]["RBCanopyVegetationType"] is not None:
            rbfactor = 0
            if rec["value"]["RBCanopyVegetationType"] in multiplicationFactors:
                rbfactor = multiplicationFactors[rec["value"]["RBCanopyVegetationType"]]

            rbunderstoryfactor = 0
            if  rec["value"]["RBUnderstoryVegetationType"] is not None and rec["value"]["RBUnderstoryVegetationType"] in multiplicationFactors:
                rbunderstoryfactor = multiplicationFactors[rec["value"]["RBUnderstoryVegetationType"]]

            result = result + (rec["value"]["RBCanopyBigTrees"] + rec["value"]["RBCanopySmallTrees"]) * rbfactor
            rbunderstoryshrubs = 0
            if rec["value"]["RBUnderstoryWoodyShrubs"] is not None:
                rbunderstoryshrubs = rec["value"]["RBUnderstoryWoodyShrubs"]
            result = result + (rbunderstoryshrubs * rbunderstoryfactor)
            count = count + 1

    if count == 0:
        return None

    return result / count


def percentBigTreeCover(visitMetrics, riparianStructures):
    if riparianStructures is None:
        visitMetrics["PercentBigTreeCover"] = None
        return

    inScope = []

    inScope.extend([s["value"]["LBCanopyBigTrees"] for s in riparianStructures["values"] if s["value"]["LBCanopyBigTrees"] is not None])
    inScope.extend([s["value"]["RBCanopyBigTrees"] for s in riparianStructures["values"] if s["value"]["RBCanopyBigTrees"] is not None])

    if inScope.__len__() > 0:
        visitMetrics["PercentBigTreeCover"] = np.mean(inScope)
    else:
        visitMetrics["PercentBigTreeCover"] = None


def percentUnderstoryCover(visitMetrics, riparianStructures):
    if riparianStructures is None:
        visitMetrics["PercentUnderstoryNoCover"] = None
        visitMetrics["PercentUnderstoryCover"] = None
        return

    inScope = []

    inScope.extend([s["value"]["LBUnderstoryCover"] for s in riparianStructures["values"] if s["value"]["LBUnderstoryCover"] is not None])
    inScope.extend([s["value"]["RBUnderstoryCover"] for s in riparianStructures["values"] if s["value"]["RBUnderstoryCover"] is not None])

    if inScope.__len__() > 0:
        understoryCover = np.mean(inScope)
        visitMetrics["PercentUnderstoryCover"] = understoryCover
        visitMetrics["PercentUnderstoryNoCover"] = 100 - understoryCover
    else:
        visitMetrics["PercentUnderstoryCover"] = None
        visitMetrics["PercentUnderstoryNoCover"] = None


def percentNonWoodyGroundCover(visitMetrics, visit, riparianStructures):
    if riparianStructures is None:
        visitMetrics["PercentNonWoodyGroundCover"] = None
        return

    inScope = []
    if visit["iterationID"] == 1:
        inScope.extend([s["value"]["LBGroundcoverNonWoodyShrubs"] + s["value"]["LBUnderstoryNonWoodyShrubs"] for s in riparianStructures["values"] if s["value"]["LBGroundcoverNonWoodyShrubs"] is not None and s["value"]["LBUnderstoryNonWoodyShrubs"] is not None])
        inScope.extend([s["value"]["RBGroundcoverNonWoodyShurbs"] + s["value"]["RBUnderstoryNonWoodyShrubs"] for s in riparianStructures["values"] if s["value"]["RBGroundcoverNonWoodyShurbs"] is not None and s["value"]["RBUnderstoryNonWoodyShrubs"] is not None])
    else:
        inScope.extend([s["value"]["LBUnderstoryNonWoodyForbesGrasses"] + s["value"]["LBGroundcoverNonWoodyForbesGrasses"] for s in riparianStructures["values"] if s["value"]["LBUnderstoryNonWoodyForbesGrasses"] is not None and s["value"]["LBGroundcoverNonWoodyForbesGrasses"] is not None])
        inScope.extend([s["value"]["RBUnderstoryNonWoodyForbesGrasses"] + s["value"]["RBGroundcoverNonWoodyForbesGrasses"] for s in riparianStructures["values"] if s["value"]["RBUnderstoryNonWoodyForbesGrasses"] is not None and s["value"]["RBGroundcoverNonWoodyForbesGrasses"] is not None])

    if inScope.__len__() > 0:
        visitMetrics["PercentNonWoodyGroundCover"] = np.mean(inScope)
    else:
        visitMetrics["PercentNonWoodyGroundCover"] = None


def percentWoodyCover(visitMetrics, riparianStructures):
    if riparianStructures is None:
        visitMetrics["PercentWoodyCover"] = None
        return

    inScope = []

    inScope.extend([s["value"]["LBWoodyCover"] for s in riparianStructures["values"] if s["value"]["LBWoodyCover"] is not None])
    inScope.extend([s["value"]["RBWoodyCover"] for s in riparianStructures["values"] if s["value"]["RBWoodyCover"] is not None])

    if inScope.__len__() > 0:
        visitMetrics["PercentWoodyCover"] = np.mean(inScope)
    else:
        visitMetrics["PercentWoodyCover"] = None


def percentGroundCover(visitMetrics, riparianStructures):
    if riparianStructures is None:
        visitMetrics["PercentGroundCover"] = None
        return

    inScope = []

    inScope.extend([s["value"]["LBGroundCover"] for s in riparianStructures["values"] if s["value"]["LBGroundCover"] is not None])
    inScope.extend([s["value"]["RBGroundCover"] for s in riparianStructures["values"] if s["value"]["RBGroundCover"] is not None])

    if inScope.__len__() > 0:
        visitMetrics["PercentGroundCover"] = np.mean(inScope)
    else:
        visitMetrics["PercentGroundCover"] = None


def percentGroundCoverNoCover(visitMetrics, riparianStructures):
    if riparianStructures is None:
        visitMetrics["PercentGroundCoverNoCover"] = None
        return

    inScope = []

    inScope.extend([s["value"]["LBGroundCoverNoCover"] for s in riparianStructures["values"] if s["value"]["LBGroundCoverNoCover"] is not None])
    inScope.extend([s["value"]["RBGroundCoverNoCover"] for s in riparianStructures["values"] if s["value"]["RBGroundCoverNoCover"] is not None])

    if inScope.__len__() > 0:
        visitMetrics["PercentGroundCoverNoCover"] = np.mean(inScope)
    else:
        visitMetrics["PercentGroundCoverNoCover"] = None


def percentCanopyNoCover(visitMetrics, riparianStructures):
    if riparianStructures is None:
        visitMetrics["PercentCanopyNoCover"] = None
        return

    inScope = []

    inScope.extend([s["value"]["LBCanopyBigTrees"] + s["value"]["LBCanopySmallTrees"] for s in riparianStructures["values"] if s["value"]["LBCanopyBigTrees"] is not None and s["value"]["LBCanopySmallTrees"] is not None])
    inScope.extend([s["value"]["RBCanopyBigTrees"] + s["value"]["RBCanopySmallTrees"] for s in riparianStructures["values"] if s["value"]["RBCanopyBigTrees"] is not None and s["value"]["RBCanopySmallTrees"] is not None])

    if inScope.__len__() > 0:
        visitMetrics["PercentCanopyNoCover"] = 100 - np.mean(inScope)
    else:
        visitMetrics["PercentCanopyNoCover"] = None

