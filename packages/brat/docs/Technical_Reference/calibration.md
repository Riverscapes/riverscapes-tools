---
title: CONUS BRAT Calibration
weight: 7
---

The following instructions describe how to parameterize BRAT within the continental United States. Users outside the United States should refer to the [international instructions]({{site.baseurl}}/Technical_Reference/international.html).

# BRAT Parameter Database

BRAT Parameters for the continental Unoted States are stored in a PostGres database. Email the BRAT development team at (info@northarrowresearch.com) for access to this database should you wish to get access to this database and contribute parameters.

The BRAT parameter database schema is a subset of the database that is generated as part of each BRAT run. When the BRAT model is run, the contents of the parameter database are used to seed a copy of the model run database on your local computer. In this way the BRAT parameter database is intended to act as the central repository of BRAT parameters for BRAT runs, and a single place where analysts can share these parameters.

Refer to the [BRAT model database documentation](database.html) for a description of the database tables. 

![BRAT Parameter Database](https://docs.google.com/drawings/d/e/2PACX-1vSKwIBhCgP7RBw6SdceOFOUrYvoOibm_e4I9-tJXGrH73aQJdwte4wP8K2MY3QnRCU6mfWqX4yquZ7M/pub?w=561&h=666)

# Software

To edit BRAT parameters you will need a piece of software that can connect to PostGres databases. We recommend [DataGrip](https://www.jetbrains.com/datagrip/) for which there is a free educational license. With DataGrip you can enter parameters using SQL statements or there are also editing tools that let you edit the data within a table, like a spreadsheet.

Contact the BRAT development team (info@norrtharrowresearch.com) for credentials to access this database.

![DataGrip]({{site.baseurl}}/assets/images/calibration/data_grip_edit_parameters.png)

# Parameters

# 4. Maximum Drainage Area

Beaver dams are washed out by high flows and so BRAT needsd to know the places in the watershed where these high flows occur. Anywhere with flows higher than the specified parameter will have the output beaver dam capacity set to zero. BRAT takes this parameter in the form of a "maximum drainage area" value as opposed to discharge. The steps to find this parameter are:

1. Open your desktop GIS software.
1. Load the stream network layer from your BRAT run:
    1. Open the [RAVE toolbar](http://rave.riverscapes.xyz)
​    1. Open the BRAT run riverscapes project file called `project.rs.xml`. It should reside in the root of the BRAT project.
​    1. Right click on the ***\*NHDFlowline\**** layer in the BRAT inputs section of the project tree and click "Add To Map".
    1. Load the best available base imagery. You can use satellite basemap in ArcGIS or add a similar [satellite layer in QGIS](https://planet.qgis.org/planet/tag/world%20imagery/).
1. Pan and zoom to find the place where water flows out of the eight digit HUC. There may be several actual streams if the outflow is braided.
1. Use the measure tool ([ArcGIS](https://desktop.arcgis.com/en/arcmap/10.3/map/working-with-layers/measuring-distances-and-areas.htm), [QGIS](https://docs.qgis.org/2.8/en/docs/user_manual/introduction/general_tools.html#measure-length-areas-and-angles)) to measure the width of the visible river channel at the watesrhed outflow.
1. Proceed upstream looking for the first reach where the width of the channel is approximately 20 m or less.
1. Use the identify tool ([ArcGIS](https://desktop.arcgis.com/en/arcmap/10.3/map/working-with-layers/identifying-features.htm), [QGIS](https://docs.qgis.org/2.8/en/docs/user_manual/introduction/general_tools.html#identify)) to query the attributes of the uppermost reach that has a width of 20 m.
1. Note down the drainage area value for this feature from the `iGeo_DA` attribute.
1. Enter the `iGeo_DA` drainage area value into the `MaxDrainageArea` column of the Google Sheet.

## Video Demonstration

<div class="responsive-embed">
<iframe src="https://www.youtube.com/embed/u6c6g1tdhak" frameborder="0" allowfullscreen></iframe>
</div>

# 5. Hydrologic Regional Equations

BRAT needs two discharge values for each reach feature within a watershed. The first is for low flow that captures whether there is sufficient water in the channel during dry periods to support a beaver dam and lodge. The second is for peak flow that captures whether a 2 year recurrance interval flood would wash out a beaver dam.

BRAT calculates these discharges using regional hydrologic equations that convert drainage area into discharge. The USGS maintains a series of publications that contain hydrologic equations for the continental United States:

[https://water.usgs.gov/osw/programs/nss/pubs.html](https://water.usgs.gov/osw/programs/nss/pubs.html)

The typical equation will be of the form where Q equals:

```
0.000133 * (DRNAREA ** 1.05) * (PRECIP) ** 2.1
```

Where `DRNAREA` represents the drainage area for a particular reach. Other parameters (such as `PRECIP` in the example given) may be present. We have calculate most of these for the Pacific northwest United States. 

Copy and paste the relevant low (QLow) and peak (Q2) formulas into the relevant columns in the aforementioned Google Sheet. Note that the syntax is Python where the syntax for raising the power is double asterix (**) and not a carrot (^).

State specific documentation can be found [here](https://riverscapes.github.io/sqlBRAT/Technical_Reference/regional_regression.html).

# 6. Vegetation Suitability

The suitability of riparian vegetation for beaver dam construction is calculated for both existing conditions as well as in the past, pre-European settlement. LandFire 2.0 Existing Vegetation Type (EVT) is used for the existing conditions and LandFire 2.0 Biophysical Settings (BPS) is used for historical context. Each possible EVT and BPS vegetation type is assigned a default suitability from 0 to 4, with 0 being unsuitable for beavers and 4 being the most suitable.

Each vegetation type might also have an override suitability within a specific ecoregion given the relative availability of other vegetation species. For example Ponderosa pine might have a default suitability of 2 given its thick bark, but in a specific dry ecoregion with few other options, Ponderosa Pine might be assigned an override suitability of 3.

Default suitabilities can be specified on the Vegetation Types tab of the Google Sheet. It can be useful to filter the vegetation types by epoch, representing either the existing EVT or historical BPS. Note that this worksheet is used by all BRAT runs in all watersheds in the continental United States. Be careful when assigning values that this affects all watersheds!

Override suitabilities can be assigned to specific ecoregions using the VegetationOverrides worksheet. Refer to the Watersheds worksheet to determine the ecoregion assignment for each watershed.

It might be helpful to review which vegetation types are present within a BRAT project. To do this open the `brat.sqlite` [database]({{site.baseurl}}/Technical_Reference/database.html) using your preferred database software and review the contents of the `vwReachVegetationTypes` view. This view summarizes the total area of all the different vegetation types present within both the 100 m and 30 m buffers adjacent to reaches within a watershed.

![vwReachVegetationTypes]({{site.baseurl}}/assets/images/calibration/vwReachVegetationTypes.png)

Information on how to import vegetation suitability values from old pyBRAT projects can be found on the page for [importing suitability values]({{site.baseurl}}/Technical_Reference/suitability.html).

# 7. Run BRAT

Once you have updated all three parameters for a particular watershed you should contact North Arrow Research to re-run BRAT for your watershed. The results will automatically be uploaded to the riverscapes Data Warehouse where you can download them and review the affects of your changes.
