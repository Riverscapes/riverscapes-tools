---
title: Riverscapes Context Data
weight: 2
---

## Topography

<h2><a name="DEM">Digital Elevation Model</a></h2>

[National Elevation Dataset](https//www.usgs.gov/core-science-systems/national-geospatial-program/national-map) (NED) Digital Elevation Models (DEM). These DEMs are downloaded from Science Base and mosaiced into a single 10m DEM that covers the riverscapes context project extent.

<h2><a name="SLOPE">Slope Analysis</a></h2>

Slope raster generated using [gdal_dem](https//gdal.org/programs/gdaldem.html) tool specifying the slope option. Units are in percent.

<h2><a name="HILLSHADE">Hillshade</a></h2>

Hillshade raster generated using the [gdal_dem](https//gdal.org/programs/gdaldem.html) tool specifying the hillshade option.

## Vegetation

<h2><a name="EXVEG">Existing Vegetation</a></h2>

[LandFire](https//landfire.gov) Existin Vegetation Type (EVT) raster: Complexes of plant communities representing NatureServe's terrestrial Ecological Systems classification. Version 2.1.0 LF REMAP 2019: Reflects change and disturbance since completion of LF 2016 Remap, including the years of 2017, 2018, and 2019. The source is a single raster for the entire nation that is downloaded and then clipped to the riverscapes project extent.

<h2><a name="HISTVEG">Historic Vegetation</a></h2>

[LandFire](https//landfire.gov) Biophysical Setting (BPS) raster: Vegetation system that may have been dominant on the landscape pre Euro-American settlement. Version 2.1.0 LF REMAP 2019: Reflects change and disturbance since completion of LF 2016 Remap, including the years of 2017, 2018, and 2019. The source is a single raster for the entire nation that is downloaded and then clipped to the riverscapes project extent.

<h2><a name="VEGCOVER">Vegetation Cover</a></h2>

[LandFire](https//landfire.gov) Existing Vegetation Cover (EVC) raster: Vertically projected percent cover of the live canopy layer for a specific area. Version 2.2.0: Reflects adjustments to vegetation and fuels since LF 2016 Remap in disturbed areas for disturbances recorded in 2017-2020. The source is a single raster for the entire nation that is downloaded and then clipped to the riverscapes project extent.

<h2><a name="VEGHEIGHT">Vegetation Height</a></h2>

[LandFire](https//landfire.gov) Existing Vegetation Height (EVH) raster: Average height of the dominant vegetation. Version 2.2.0: Reflects adjustments to vegetation and fuels since LF 2016 Remap in disturbed areas for disturbances recorded in 2017-2020. The source is a single raster for the entire nation that is downloaded and then clipped to the riverscapes project extent.

<h2><a name="HDIST">Historic Disturbance</a></h2>

[LandFire](https//landfire.gov) Historic Disturbance (HDst) raster: The latest 10 years of Annual Disturbance products are used to identify disturbance year, type, and severity. Starting with LF Remap, HDist replaces VDist from previous LF versions incorporating pre-disturbance vegetation logic (based on disturbance year and vegetation type). The source is a single raster for the entire nation that is downloaded and then clipped to the riverscapes project extent.

<h2><a name="FDIST">Fuel Disturbance</a></h2>

[LandFire](https//landfire.gov) Fuel Disturbance (FDst) raster: The latest 10 years of Annual Disturbance products representing disturbance year and original disturbance code. FDist was a refinement of VDist in LF 1.x products and is a refinement of Historical Disturbance in LF Remap to more accurately represent disturbance scenarios within the fuels environment. The source is a single raster for the entire nation that is downloaded and then clipped to the riverscapes project extent.

<h2><a name="FCCS">Fuel Characteristic Classification System</a></h2>

[LandFire](https//landfire.gov) Fuel Characteristic Classification System (FCCS) raster: Describes the phsical characteristics of a relatively uniform unit on a lanscape that represents a distinct fire environment; provides land managers, regulators, and scientists with a nationally consistent and durable procedure to characterize and classify fuelbed characteristics to predict fuel consumption and smoke production. For LF Remap, FCCS will be released as part of the final release for CONUS. The source is a single raster for the entire nation that is downloaded and then clipped to the riverscapes project extent.

<h2><a name="VEGCONDITION">Vegetation Condition</a></h2>

[LandFire](https//landfire.gov) VCC raster: A discrete metric that quantifies the amount that current vegetation has departed from the simulated historical vegetation reference conditions. The source is a single raster for the entire nation that is downloaded and then clipped to the riverscapes project extent.

<h2><a name="VEGDEPARTURE">Vegetation Departure</a></h2>

[LandFire](https//landfire.gov) VDEP raster: Range from 0-100 depicting the amount that current vegetation has departed from simulated historical vegetation reference. The source is a single raster for the entire nation that is downloaded and then clipped to the riverscapes project extent.

<h2><a name="SCLASS">Succession Classes</a></h2>

[LandFire](https//landfire.gov) SCla raster: Current vegetation conditions with respect to vegetation species composition, cover, and height ranges of successional states occurring within each biophysical setting. The source is a single raster for the entire nation that is downloaded and then clipped to the riverscapes project extent.

## Land Management

<h2><a name="Ownership">Land Ownership</a></h2>

Land ownership obtained from the [Bureau of Management (BLM) Land Surface Agency](https//catalog.data.gov/dataset/blm-national-surface-management-agency-area-polygons-national-geospatial-data-asset-ngda). A single ShapeFile was downloaded and then pre-processed to remove invalid geometries and other irregularities.

<h2><a name="FAIRMARKETVALUE">Fair Market Value</a></h2>

[Fair Market Value](https//orcid.org/0000-0001-7827-689X) raster. This raster was downloaded and converted to US dollars per hectare.

## Ecoregions

<h2><a name="Ecoregions">Ecoregions</a></h2>

Level IV ecoregions (also contains levels I-III) obtained from the [Environmental Protection Agency](https//www.epa.gov/eco-research/ecoregions) (EPA).

## Climate

<h2><a name="Precip">Precipitation</a></h2>

[PRISM](https//prism.oregonstate.edu/normals) 30 year normal precipitation raster. Normals are baseline datasets describing average monthly and annual conditions over the most recent three full decades.

<h2><a name="MeanTemp">Mean Temperature</a></h2>

[PRISM](https//prism.oregonstate.edu/normals) 30 year normal mean temperature raster. Normals are baseline datasets describing average monthly and annual conditions over the most recent three full decades.

<h2><a name="MinTemp">Minimum Temperature</a></h2>

[PRISM](https//prism.oregonstate.edu/normals) 30 year normal minimum temperature raster. Normals are baseline datasets describing average monthly and annual conditions over the most recent three full decades.

<h2><a name="MaxTemp">Maximum Temperature</a></h2>

[PRISM](https//prism.oregonstate.edu/normals) 30 year normal maximum temperature raster. Normals are baseline datasets describing average monthly and annual conditions over the most recent three full decades.

<h2><a name="MeanDew">Mean Dew Point Temperature</a></h2>

[PRISM](https//prism.oregonstate.edu/normals) 30 year normal mean dew point temperature raster. Normals are baseline datasets describing average monthly and annual conditions over the most recent three full decades.

<h2><a name="MinVap">Minimum Vapor Pressure Deficit</a></h2>

[PRISM](https//prism.oregonstate.edu/normals) 30 year normal minimum vapor pressure deficit raster. Normals are baseline datasets describing average monthly and annual conditions over the most recent three full decades.

 <h2><a name="MaxVap">Maximum Vapor Pressure Deficit</a></h2>

[PRISM](https//prism.oregonstate.edu/normals) 30 year normal maximum vapor pressure deficit raster. Normals are baseline datasets describing average monthly and annual conditions over the most recent three full decades.

## Hydrology

<h2><a name="NHDFlowline">Flow line Network</a></h2>

[National Hydrologic Dataset (NHD) High Resolution](https//www.usgs.gov/core-science-systems/ngp/national-hydrography/nhdplus-high-resolution). 124,000 stream network polyline feature class. This is the original data downloaded as an ESRI file geodatabase and then converted to an open source GeoPackage.

 <h2><a name="buffered_clip100">100m Buffered Extent</a></h2>

The riverscapes context project extent polygon buffered by 100m.

 <h2><a name="buffered_clip500">500m Buffered Extent</a></h2>

The riverscapes context project extent polygon buffered by 500m.

<h2><a name="network_300m"></a></h2>

This polyline feature class is the original NHDPlusHR flow line network segmented into 300m reach lengths. Segmentation that would produce a feature of less than 50m is not performed. And features that are less than 50m are left untouched.

<h2><a name="network_intersected_300m">Segmented Flow line Network Intersected with Infrastructure</a></h2>

This polyline feeature class is the NETWORK300M featureclass intersected with road and rail crossings and ownership.

