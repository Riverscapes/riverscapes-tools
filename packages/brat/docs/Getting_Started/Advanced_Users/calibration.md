---
title: CONUS BRAT Calibration
weight: 7
---

The following instructions describe how to parameterize BRAT within the continental United States. Users outside the United States should refer to the [international instructions]({{site.baseurl}}/Technical_Reference/international.html).

# BRAT Parameter Database

BRAT Parameters for the continental United States are stored in a PostGres database hosted in the cloud. Email the BRAT development team at (info@northarrowresearch.com) for access to this database should you wish to get access to this database and contribute parameters.

The BRAT parameter database schema is a subset of the database that is generated as part of each BRAT run. When the BRAT model is run, the contents of the parameter database are used to seed a copy of the model run database on your local computer. In this way the BRAT parameter database is intended to act as the central repository of BRAT parameters for BRAT runs, and a single place where analysts can share these parameters.

Refer to the [BRAT model database documentation](database.html) for a description of the database tables. 

![BRAT Parameter Database](https://docs.google.com/drawings/d/e/2PACX-1vSKwIBhCgP7RBw6SdceOFOUrYvoOibm_e4I9-tJXGrH73aQJdwte4wP8K2MY3QnRCU6mfWqX4yquZ7M/pub?w=1358&h=988)

# Software

To edit BRAT parameters you will need a piece of software that can connect to the PostGres database. We recommend [DataGrip](https://www.jetbrains.com/datagrip/) for which there is a free educational license. With DataGrip you can enter parameters using SQL statements or directly edit the database tools, much like a spreadsheet:

![DataGrip]({{site.baseurl}}/assets/images/calibration/data_grip_edit_parameters.png)


In DataGrip (like most database software), data edits do not get saved back to the database until you click "submit changes" (aka "commit"). **Remember to commit your changes to Postgres by clicking the up arrow icon on the toolbar after every change.**


# Parameterizing BRAT for a Watershed

BRAT is always parameterized at the HUC8 watershed scale. If you want to use the same parameters for more than one HUC8 then you have to manually copy the values from one HUC8 to another (see section below).

The following steps describe the process of parameterizing BRAT for a single HUC. It describes the approach of editing the PostGres tables in DataGrip. Alternatively you can perform these steps in GIS (by editing attribute tables in either QGIS or ArcGIS) or, for more advanced users, using SQL statements.

## 1. Identify your HUC

In DataGrip, expand the following items in the database panel on the left:

```
brat_paramaters -> public -> tables
```

Double click the `watersheds` table to open it in grid view. This might take a few seconds if the database is sleeping and needs to wake up. You should see a table like the screenshot image above.

This table lists all HUC8s in the continental United States. Filter the table to the one watershed that you want to calibrate. Above the table in the "WHERE" box enter the following, replacing the `xxxxxxxx` with your eight digit HUC code.

```sql
watershed_id = 'xxxxxxxx'
```

If you want to see multiple watersheds you can use the percent sign as a wildcard. The first filter below returns all watersheds that start with 1701. The second filter returns all watersheds in Montana, North Dakota and South Dakota.

```sql
watershed_id = '1701%'
states ilike any(array['%mt%','%nd%','%sd%']) 
```

## 2. Maximum Drainage Area

Beaver dams are washed out by high flows and so BRAT needsd to know the places in the watershed where these high flows occur. Anywhere with flows higher than the specified parameter will have the output beaver dam capacity set to zero. BRAT takes this parameter in the form of a "maximum drainage area" value as opposed to discharge. The steps to find this parameter are:

1. Open your desktop GIS software.
1. Load the stream network layer from your BRAT run:
    1. Open the [RAVE toolbar](http://rave.riverscapes.xyz).
    1. Open the BRAT run riverscapes project file called `project.rs.xml`. It should reside in the root of the BRAT project.
    1. Right click on the ***\*NHDFlowline\**** layer in the BRAT inputs section of the project tree and click "Add To Map".
    1. Load the best available base imagery. You can use satellite basemap in ArcGIS or add a similar [satellite layer in QGIS](https://planet.qgis.org/planet/tag/world%20imagery/).
1. Pan and zoom to find the place where water flows out of the eight digit HUC. There may be several actual streams if the outflow is braided.
1. Use the measure tool ([ArcGIS](https://desktop.arcgis.com/en/arcmap/10.3/map/working-with-layers/measuring-distances-and-areas.htm), [QGIS](https://docs.qgis.org/2.8/en/docs/user_manual/introduction/general_tools.html#measure-length-areas-and-angles)) to measure the width of the visible river channel at the watesrhed outflow.
1. Proceed upstream looking for the first reach where the width of the channel is approximately 20 m or less.
1. Use the identify tool ([ArcGIS](https://desktop.arcgis.com/en/arcmap/10.3/map/working-with-layers/identifying-features.htm), [QGIS](https://docs.qgis.org/2.8/en/docs/user_manual/introduction/general_tools.html#identify)) to query the attributes of the uppermost reach that has a width of 20 m.
1. Note down the drainage area value for this feature from the `iGeo_DA` attribute.
1. Enter the `iGeo_DA` drainage area value into the `max_drainage` column of the watersheds table in PostGres.

**Remember to commit your changes to Postgres by clicking the up arrow icon on the toolbar after every change.**

## Video Demonstration

<div class="responsive-embed">
<iframe src="https://www.youtube.com/embed/u6c6g1tdhak" frameborder="0" allowfullscreen></iframe>
</div>

# 2. Hydrologic Regional Equations

BRAT needs two discharge values for each reach feature within a watershed. The first is for low flow that captures whether there is sufficient water in the channel during dry periods to support a beaver dam and lodge. The second is for peak flow that captures whether a 2 year recurrance interval flood would wash out a beaver dam.

BRAT calculates these discharges using regional hydrologic equations that convert drainage area into discharge. The USGS maintains a series of publications that contain hydrologic equations for the continental United States:

[https://water.usgs.gov/osw/programs/nss/pubs.html](https://water.usgs.gov/osw/programs/nss/pubs.html)

The typical equation will be of the form where discharge (Q) equals:

```
0.000133 * (DRNAREA ** 1.05) * (PRECIP) ** 2.1
```

Where `DRNAREA` represents the drainage area for a particular reach. Other parameters (such as `PRECIP` in the example given) may be present.

Copy and paste the relevant low (QLow) and peak (Q2) formulas into the columns `q_low` and `q2` columns respecitively. Note that the syntax is Python for raising the power is double asterix (**) and not a carrot (^).

State specific documentation can be found [here](https://riverscapes.github.io/sqlBRAT/Technical_Reference/regional_regression.html).

**Remember to commit your changes to Postgres by clicking the up arrow icon on the toolbar after every change.**

# 3. Hydrologic Equation Parameters

When a hydrological equation for a particular watershed refers to a parameter such as precipitation (PRECIP) or slope (SLOPE), the database must also contain a corresponding value for that parameter in that watershed.

1. The first step is to confirm that the database already contains a definition for each hydrological parameter. Open the `hydro_params` table in DataGrip and scroll to confirm that the parameter referred you've used in a hydrological equation is already present.
    1. If it is not, then add a new record being careful to provide a unique `name`.
    1. Commit the change.
1. Note down the param_id of the hydrological parameter.
1. Open the `watershed_hydro_params` table and add a new record. Enter the `watershed_id` and the `param_id` from the previous step, then enter the relevant watershed parameter value.
1. Repeat for all parameters mentioned in the hydrological equations for the watershed.

It can be helpful to see all parameters for a watershed using the where filter `watershed_id = 'xxxxxxxx'`


# 4. Vegetation Suitability

The suitability of riparian vegetation for beaver dam construction is calculated for both existing conditions as well as in the past, pre-European settlement. LandFire 2.0 Existing Vegetation Type (EVT) is used for the existing conditions and LandFire 2.0 Biophysical Settings (BPS) is used for historical context. Each possible EVT and BPS vegetation type is assigned a default suitability from 0 to 4, with 0 being unsuitable for beavers and 4 being the most suitable.

Each vegetation type might also have an override suitability within a specific ecoregion given the relative availability of other vegetation species. For example Ponderosa pine might have a default suitability of 2 given its thick bark, but in a specific dry ecoregion with few other options, Ponderosa Pine might be assigned an override suitability of 3.

Default suitabilities can be specified `vegetation_types` database table. It can be useful to filter the vegetation types by epoch, representing either the existing EVT or historical BPS. Note that this database table is used by all BRAT runs in all watersheds in the continental United States. Be careful when assigning values that this affects all watersheds!

Override suitabilities can be assigned to specific ecoregions using the `vegetation_overrides` database table. Refer to the `ecoregion_id` column of the `watersheds` table to determine the ecoregion assignment for each watershed.

Remember to use the BRAT report that accompanies each sqlBRAT run and refer to the vegetation section that lists the most prevalent vegetation types in each watershed. This report section summarizes the total area of all the different vegetation types present within both the 100 m and 30 m buffers adjacent to reaches within a watershed.

Information on how to import vegetation suitability values from old pyBRAT projects can be found on the page for [importing suitability values]({{site.baseurl}}/Technical_Reference/suitability.html).

# 7. Run BRAT

Once you have updated all three parameters for a particular watershed you should contact North Arrow Research to re-run BRAT for your watershed. The results will automatically be uploaded to the riverscapes Data Warehouse where you can download them and review the affects of your changes.

# Useful BRAT Parameter Database Commands

You can execute these commands by right clicking on the database node in DataGrip and choosing to open a new Console. Cut and paste in the following commands and then tweak as necessary.

To update the max drainage value of a particular HUC:

```sql
UPDATE watersheds SET max_drainage = 1234.5678 WHERE watershed_id = '17050001'
```

To update the default suitability to zero for the existing vegetation type *Alaska Arctic Tidal Marsh* which has the LandFire ID of 2222:

```sql
UPDATE vegetation_types SET default_suitability = 0 WHERE vegetation_id = 2222
```

To update default suitabilities for several vegetation types:

```sql
UPDATE vegetation_types SET default_suitability = 3 WHERE vegetation_id IN (1234, 4567, 7777)
```

To add an override vegetation suitability for a specific vegetation type and ecoregion:

```sql
INSERT INTO vegetation_overrides (vegetation_id, ecoregion_it, override_suitability) VALUES (2222, 14, 0);
```

To find all the vegetation overrides for a particular ecoregion:

```sql
SELECT * FROM vegetation_overrides WHERE ecoregion_id = 14
```

To delete a particular vegetation override:

```sql
DELETE FROM vegetation_overrides WHERE vegetation_id = 2222 and ecoregion_id = 14
```
