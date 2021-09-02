---
title: Riverscapes Context Data
---

# Topography

<h2><a name="DEM">Digital Elevation Model</a></h2>

[National Elevation Dataset](https//www.usgs.gov/core-science-systems/national-geospatial-program/national-map) (NED) Digital Elevation Models (DEM). These DEMs are downloaded from Science Base and mosaiced into a single 10m DEM that covers the riverscapes context project extent.

<h2><a name="FA">Flow Accumulation</a></h2>

Flow accumulation raster generated using [pyGeoProcessing](https//pypi.org/project/pygeoprocessing). Raster values are cell counts. The mosaiced DEM is used as the source raster for this process.

<h2><a name="DA">Drainage Area</a></h2>

Drainage area raster generated from the flow accumulation raster. This raster is produced by multiplying the flow accumulation raster by the area of a single pixel and then converting to square kilometers.

<h2><a name="HILLSHADE">Hillshade</a></h2>

Hillshade raster generated using the [gdal_dem](https//gdal.org/programs/gdaldem.html) tool specifying the hillshade option.

<h2><a name="SLOPE">Slope</a></h2> 

Slope raster generated using [gdal_dem](https//gdal.org/programs/gdaldem.html) tool specifying the slope option. Units are in percent.

<h2><a name="HAND">Height Above Nearest Drainage (HAND)</a></h2> 

[Height above nearest drainage](https//hydrology.usu.edu/taudem/taudem5) (HAND) raster downloaded from the University of Texas. These data were produced for the Continental United States at the HUC6 scale.

# Vegetation

<h2><a name="EXVEG">Existing Vegetation</a></h2> 

[LandFire](https//landfire.gov) LF REMAP 2019 Existing Vegetation Type (EVT) Raster.The source is a single raster for the entire nation that is downloaded and then clipped to the riverscapes project extent.

<h2><a name="HISTVEG">Historic Vegetation</a></h2> 

[LandFire](https//landfire.gov) LF REMAP 2019 Historic Biophysical Settings (BPS) Raster. The source is a single raster for the entire nation that is downloaded and then clipped to the riverscapes project extent.

# Infrastructure

<h2><a name="OWNERSHIP">Land Ownership</a></h2> 

Land ownership obtained from the [Bureau of Management (BLM) Land Surface Agency](https//catalog.data.gov/dataset/blm-national-surface-management-agency-area-polygons-national-geospatial-data-asset-ngda). A single ShapeFile was downloaded and then pre-processed to remove invalid geometries and other irregularities.

<h2><a name="FAIR_MARKET">Fair Market Value</a></h2> 

[Fair Market Value](https//orcid.org/0000-0001-7827-689X) raster. This raster was downloaded and converted to US dollars per hectare.

<h2><a name="ECOREGIONS">Ecoregions</a></h2> 

Level III ecoregions obtained from the [Environmental Protection Agency](https//www.epa.gov/eco-research/ecoregions) (EPA).

# Climate 

<h2><a name="PPT">Precipitation</a></h2> 

[PRISM](https//prism.oregonstate.edu/normals) 30 year normal precipitation raster. Normals are baseline datasets describing average monthly and annual conditions over the most recent three full decades.

<h2><a name="TMEAN">Mean Temperature</a></h2> 

[PRISM](https//prism.oregonstate.edu/normals) 30 year normal mean temperature raster. Normals are baseline datasets describing average monthly and annual conditions over the most recent three full decades.

<h2><a name="TMIN">Minimum Temperature</a></h2> 

[PRISM](https//prism.oregonstate.edu/normals) 30 year normal minimum temperature raster. Normals are baseline datasets describing average monthly and annual conditions over the most recent three full decades.

<h2><a name="TMAX">Maximum Temperature</a></h2> 

[PRISM](https//prism.oregonstate.edu/normals) 30 year normal maximum temperature raster. Normals are baseline datasets describing average monthly and annual conditions over the most recent three full decades.

<h2><a name="TDMEAN">Mean Dew Point Temperature</a></h2> 

[PRISM](https//prism.oregonstate.edu/normals) 30 year normal mean dew point temperature raster. Normals are baseline datasets describing average monthly and annual conditions over the most recent three full decades.

<h2><a name="VPDMIN">Minimum Vapor Pressure Deficit</a></h2> 

[PRISM](https//prism.oregonstate.edu/normals) 30 year normal minimum vapor pressure deficit raster. Normals are baseline datasets describing average monthly and annual conditions over the most recent three full decades.

 <h2><a name="VPDMAX">Maximum Vapor Pressure Deficit</a></h2> 

[PRISM](https//prism.oregonstate.edu/normals) 30 year normal maximum vapor pressure deficit raster. Normals are baseline datasets describing average monthly and annual conditions over the most recent three full decades.

# Hydrology

<h2><a name="NETWORK">Flow line Network</a></h2> 

[National Hydrologic Dataset (NHD) High Resolution](https//www.usgs.gov/core-science-systems/ngp/national-hydrography/nhdplus-high-resolution). 124,000 stream network polyline feature class. This is the original data downloaded as an ESRI file geodatabase and then converted to an open source GeoPackage.

 <h2><a name="BUFFEREDCLIP100">100m Buffered Extent</a></h2> 

The riverscapes context project extent polygon buffered by 100m.

 <h2><a name="BUFFEREDCLIP500">500m Buffered Extent</a></h2> 

The riverscapes context project extent polygon buffered by 500m.


This polyline feature class is the original NHDPlusHR flow line network segmented into 300m reach lengths. Segmentation that would produce a feature of less than 50m is not performed. And features that are less than 50m are left untouched.

<h2><a name="NETWORK300M_INTERSECTION ">Segmented Flow line Network Intersected with Infrastructure</a></h2> 

This polyline feeature class is the NETWORK300M featureclass intersected with road and rail crossings and ownership.
 
<h2><a name="BANKFULL_CHANNEL ">Bankfull Channel</a></h2> 
docs 
Estimated bankfull channel polygon feature class. Polygons are generated by buffering the NHDPlus HR flow lines using the equation presented in [Beechie and Imaki (2013)](http//dx.doi.org/10.1002/2013WR013629). This equation uses drainage area (obtained from the NHDPlusHR Value Added Attribute (VAA) table with PRISM precipitation raster data.
