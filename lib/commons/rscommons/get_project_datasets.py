import re
import xml.etree.ElementTree as ET


def get_project_datasets(project_xml: str) -> list:
    """Parses a project xml (project.rs.xml) to get lists of the datasets contained in a project.

    Args:
        project_xml (str): path to the project.rs.xml file

    Returns:
        lists: returns 3 lists: floating point raster datasets, categorical raster datasets, and vector datasets.
        Lists are of the form [[dataset_path, dataset_id]]
    """

    tree = ET.parse(project_xml)
    root = tree.getroot()

    # separate categorical rasters from continous. Is there more than these two?
    categorical = ['EXVEG', 'HISTVEG', 'EXVEG_SUIT', 'HISTVEG_SUIT', 'FCCS', 'FDIST', 'HDIST', 'SCLASS', 'VEGCOVER', 'VEGHEIGHT', 'VEGCONDITION', 'VEGDEPARTURE']

    realizations = root.find('Realizations')  # this assumes 1 realization, will there be cases where there's more...?
    realization = realizations.find('Realization')

    # rasters
    ds = realization.find('Datasets')
    if ds:
        raster = ds.findall('Raster')
        if raster:
            float_datasets = [[raster[i].find('Path').text, raster[i].attrib['id']] for i in range(len(raster)) if raster[i].attrib['id'] not in categorical]
            cat_datasets = [[raster[i].find('Path').text, raster[i].attrib['id']] for i in range(len(raster)) if raster[i].attrib['id'] in categorical]
        vector = ds.findall('Vector')
        if vector:
            vector_datasets = [[vector[i].find('Path').text, vector[i].attrib['id']] for i in range(len(vector)) if not re.search('WBD.+', vector[i].attrib['id'])]
    else:
        float_datasets = []
        cat_datasets = []
        vector_datasets = []

    inputs = realization.find('Inputs')
    if inputs:
        inputraster = inputs.findall('Raster')
    else:
        inputraster = None
    intermediates = realization.find('Intermediates')
    if intermediates:
        interraster = intermediates.findall('Raster')
    else:
        interraster = None
    outputs = realization.find('Outputs')
    if outputs:
        outputraster = outputs.findall('Raster')
    else:
        outputraster = None

    if inputraster:
        for i, rasterin in enumerate(inputraster):
            if rasterin.attrib['id'] in categorical:
                if [rasterin.find('Path').text, rasterin.attrib['id']] not in cat_datasets:
                    cat_datasets.append([rasterin.find('Path').text, rasterin.attrib['id']])
            else:
                if [rasterin.find('Path').text, rasterin.attrib['id']] not in float_datasets:
                    float_datasets.append([rasterin.find('Path').text, rasterin.attrib['id']])

    if interraster:
        for i, rasterint in enumerate(interraster):
            if rasterint.attrib['id'] in categorical:
                if [rasterint.find('Path').text, rasterint.attrib['id']] not in cat_datasets:
                    cat_datasets.append([rasterint.find('Path').text, rasterint.attrib['id']])
            else:
                if [rasterint.find('Path').text, rasterint.attrib['id']] not in float_datasets:
                    float_datasets.append([rasterint.find('Path').text, rasterint.attrib['id']])

    if outputraster:
        for i, rasterout in enumerate(outputraster):
            if rasterout.attrib['id'] in categorical:
                if [rasterout.find('Path').text, rasterout.attrib['id']] not in cat_datasets:
                    cat_datasets.append([rasterout.find('Path').text, rasterout.attrib['id']])
            else:
                if [rasterout.find('Path').text, rasterout.attrib['id']] not in float_datasets:
                    float_datasets.append([rasterout.find('Path').text, rasterout.attrib['id']])

    # INPUT DEM (should be depracated now with new projectxml)
    if inputs:
        if inputs.find('DEM'):
            if len(inputs.find('DEM')) > 0:
                dem = inputs.find('DEM')
                float_datasets.append([dem.find('Path').text, dem.attrib['id']])

    # vectors
    if inputs:
        inputvector = inputs.findall('Vector')
    else:
        inputvector = None
    if intermediates:
        intervector = intermediates.findall('Vector')
    else:
        intervector = None
    if outputs:
        outputvector = outputs.findall('Vector')
    else:
        outputvector = None

    if inputvector:
        for i, vectorin in enumerate(inputvector):
            if [vectorin.find('Path').text, vectorin.attrib['id']] not in vector_datasets:
                vector_datasets.append([vectorin.find('Path').text, vectorin.attrib['id']])

    if intervector:
        for i, vectorint in enumerate(intervector):
            if [vectorint.find('Path').text, vectorint.attrib['id']] not in vector_datasets:
                vector_datasets.append([vectorint.find('Path').text, vectorint.attrib['id']])

    if outputvector:
        for i, vectorout in enumerate(outputvector):
            if [vectorout.find('Path').text, vectorout.attrib['id']] not in vector_datasets:
                vector_datasets.append([vectorout.find('Path').text, vectorout.attrib['id']])

    # append layers inside of geopackages to lists
    if ds:
        geopackage = ds.findall('Geopackage')
        if geopackage:
            for g in geopackage:
                lyr = g.find('Layers')
                gpvec = lyr.findall('Vector')
                gprast = lyr.findall('Raster')
                if len(gpvec) > 0:
                    for vec in gpvec:
                        vector_datasets.append([g.find('Path').text + '/' + vec.attrib['lyrName'], vec.attrib['lyrName']])
                if len(gprast) > 0:
                    for rast in gprast:
                        float_datasets.append([g.find('Path').text + '/' + rast.attrib['lyrName'], rast.attrib['lyrName']])  # check for categorical rasters in geopackages

    if inputs:
        inputgeopackage = inputs.findall('Geopackage')
        if inputgeopackage:
            for g in inputgeopackage:
                lyr = g.find('Layers')
                gpvec = lyr.findall('Vector')
                gprast = lyr.findall('Raster')
                if len(gpvec) > 0:
                    for vec in gpvec:
                        vector_datasets.append([g.find('Path').text + '/' + vec.attrib['lyrName'], vec.attrib['lyrName']])
                if len(gprast) > 0:
                    for rast in gprast:
                        float_datasets.append([g.find('Path').text + '/' + rast.attrib['lyrName'], rast.attrib['lyrName']])

    if intermediates:
        intergeopackage = intermediates.findall('Geopackage')
        if intergeopackage:
            for g in intergeopackage:
                lyr = g.find('Layers')
                gpvec = lyr.findall('Vector')
                gprast = lyr.findall('Raster')
                if len(gpvec) > 0:
                    for vec in gpvec:
                        vector_datasets.append([g.find('Path').text + '/' + vec.attrib['lyrName'], vec.attrib['lyrName']])
                if len(gprast) > 0:
                    for rast in gprast:
                        float_datasets.append([g.find('Path').text + '/' + rast.attrib['lyrName'], rast.attrib['lyrName']])

    if outputs:
        outputgeopackage = outputs.findall('Geopackage')
        if outputgeopackage:
            for g in outputgeopackage:
                lyr = g.find('Layers')
                gpvec = lyr.findall('Vector')
                gprast = lyr.findall('Raster')
                if len(gpvec) > 0:
                    for vec in gpvec:
                        vector_datasets.append([g.find('Path').text + '/' + vec.attrib['lyrName'], vec.attrib['lyrName']])
                if len(gprast) > 0:
                    for rast in gprast:
                        float_datasets.append([g.find('Path').text + '/' + rast.attrib['lyrName'], rast.attrib['lyrName']])

    return float_datasets, cat_datasets, vector_datasets
