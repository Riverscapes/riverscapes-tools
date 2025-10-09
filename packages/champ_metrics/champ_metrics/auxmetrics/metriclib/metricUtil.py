import datetime

__version__ = '0.1.0'

def populateDefaultColumns(dict, visitid):
    dict["VisitID"] = visitid
    dict["EngineVersion"] = __version__
    dict["CalcDate"] = datetime.datetime.utcnow().isoformat() + 'Z'