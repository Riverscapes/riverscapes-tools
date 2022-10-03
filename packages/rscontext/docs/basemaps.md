---
title: Basemaps
weight: 3
---

Basemaps can provide important contextual information for riverscapes projects in addition to what the layers in the Riverscapes Context projects provide. [QRAVE](https://rave.riverscapes.xyz/Download/install_qrave.html) and [ArcRAVE](https://rave.riverscapes.xyz/Download/install_arcrave.html) provide access to various basemaps that can be displayed with projects to provide this context. There is a `Basemaps` folder within the RAVE project tree, where all of the available basemaps can be found and added to maps. This page contains descriptions of each of the available basemaps, including their sources.

## Basemaps available in ArcRAVE
(links lead to descriptions on this page)
- [USGS hydrography]({{site.baseurl}}/basemaps.html#usgs-hydrography)
- [USGS Topography]({{site.baseurl}}/basemaps.html#usgs-topography)
- [Cartographic Basics]({{site.baseurl}}/basemaps.html#cartographic-basics)
- [NAIP imagery]({{site.baseurl}}/basemaps.html#naip)
- [Geology]({{site.baseurl}}/basemaps.html#geology)

## Basemaps available in QRAVE
(links lead to descriptions on this page)
- [Google basemaps]({{site.baseurl}}/basemaps.html#google)
- [Bing Imagery]({{site.baseurl}}/basemaps.html#bing-maps)
- [USGS hydrography]({{site.baseurl}}/basemaps.html#usgs-hydrography)
- [USGS Topography]({{site.baseurl}}/basemaps.html#usgs-topography)
- [Cartographic Basics]({{site.baseurl}}/basemaps.html#cartographic-basics)
- [NAIP imagery]({{site.baseurl}}/basemaps.html#naip)
- [Geology]({{site.baseurl}}/basemaps.html#geology)

## Basemap Descriptions

### [Google](https://www.google.com/maps)

- **Google Imagery** - Google's mosaic of aerial imagery used in Google Earth and Google Maps. It is composed of satellite imagery collected by a variety of satellite companies, and is continuously updated as new imagery is collected.

<img src="{{site.baseurl}}/assets/images/google_imagery.png" width="560" height="315">

- **Google Terrain** - Google's shaded relief topographic map, which includes elevation contours and shade based on terrain overlain on their main basemap (see below).

<img src="{{site.baseurl}}/assets/images/google_terrain.png" width="560" height="315">

- **Google maps** - The default basemap for Google Maps, which incorporates green coloration for forest cover, blue for water, as well as transportation and infrastructure features.

<img src="{{site.baseurl}}/assets/images/google_maps.png" width="560" height="315">

### [Bing Maps](https://www.bing.com/maps/)

- **Bing Aerial Imagery** - Bing's mosaic of aerial imagery used for Bing Maps. Similarly to Google, Bing collects satellite images from various sources and mosaics them together into a seamless basemap, which is continuously updated as new imagery is acquired.

<img src="{{site.baseurl}}/assets/images/bing_imagery.png" width="560" height="315">

### [USGS Hydrography](https://www.usgs.gov/national-hydrography/access-national-hydrography-products)

- **Watershed Boundary Dataset (WBD)** - From the USGS: The Watershed Boundary Dataset (WBD) is a seamless, national hydrologic unit dataset. Hydrologic units represent the area of the landscape that drains to a portion of the stream network. More specifically, a hydrologic unit defines the areal extent of surface water drainage to an outlet point on a dendritic stream network or to multiple outlet points where the stream network is not dendritic. A hydrologic unit may represent all or only part of the total drainage area to an outlet point so that multiple hydrologic units may be required to define the entire drainage area at a given outlet. Hydrologic unit boundaries in the WBD are determined based on topographic, hydrologic, and other relevant landscape characteristics without regard for administrative, political, or jurisdictional boundaries. The WBD seamlessly represents hydrologic units at six required and two optional hierarchical levels.

See additonal documentation [here](https://www.usgs.gov/national-hydrography/watershed-boundary-dataset)

<img src="{{site.baseurl}}/assets/images/wbd.png" width="560" height="315">

- **Drainage Network NHD** - From the USGS: The National Hydrography Dataset (NHD) represents the water drainage network of the United States with features such as rivers, streams, canals, lakes, ponds, coastline, dams, and streamgages. The NHD is the most up-to-date and comprehensive hydrography dataset for the Nation.

The dataset consists of:
- *Flowlines*, which represent features like streams and canals
- *Area* features, which represent features such as large rivers that aren't accurately represented using a simple line (a flowline called an artificial path creates a line feature associated with the area polygons)
- *Waterbody* features, which represent lakes, ponds, and reservoirs.

See the [**legend**](https://hydro.nationalmap.gov/arcgis/rest/services/nhd/MapServer/legend) for the symbolization of features in this basemap, and [**additional documentation**](https://www.usgs.gov/national-hydrography/national-hydrography-dataset?qt-science_support_page_related_con=0#qt-science_support_page_related_con).

<img src="{{site.baseurl}}/assets/images/nhd_basemap.png" width="560" height="315">

### [USGS Topography](https://basemap.nationalmap.gov/arcgis/rest/services/USGSTopo/MapServer)
The USGS Topography and Cartographic Basics basemaps are sourced from data with the USGS [National Map](https://www.usgs.gov/programs/national-geospatial-program/national-map), and other public domain data.

- **Topo Base Map** - This basemap is a digitized version of the USGS topographic quadrangle maps.

<img src="{{site.baseurl}}/assets/images/topo.png" width="560" height="315">

- **Shaded Relief** (not working)

- **Contours** - This base map adds elevation contour lines as a basemap.

<img src="{{site.baseurl}}/assets/images/contours.png" width="560" height="315">

- **NED 3DEPE** (not working)

### Cartographic Basics

- **Boundaries** - this basemap brings in automatically symbolized boundaries for various types of geographic areas including:
  - State or Territory
  - County or Equivalent
  - Congressional District
  - Bureau of Land Management
  - Tennessee Valley Authority
  - Military Reserve
  - National Cemetery
  - National Grassland
  - US Fish & Wildlife Service
  - National Wilderness
  - National Forest
  - National Park
  - Native American Area
  - Minor Civil Division
  - Unincorporated Place
  - Incorporated Place

  The map includes the actual boundaries and their labels separately.
  See the [**government unit**](https://cartowfs.nationalmap.gov/arcgis/rest/services/govunits/MapServer/legend) and [**other boundaries**](https://cartowfs.nationalmap.gov/arcgis/rest/services/selectable_polygons/MapServer/legend) legends for the symbology used.

  <img src="{{site.baseurl}}/assets/images/boundaries.png" width="560" height="315">

- **Geographic Place Names** - This basemap adds to maps symbolized points representing various types of places along with labels with the names of those places.

<img src="{{site.baseurl}}/assets/images/place_names.png" width="560" height="315">

- **Transportation** - The transportation basemap adds representations of transportation infrastructure to the map. These include things like roads, railroads, airport and runways, ferries, and trails. These features are from the [TIGER/Line](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html) data provided through U.S. Census Bureau and road data from U.S. Forest Service.

See the [**transportation legend**](https://cartowfs.nationalmap.gov/arcgis/rest/services/transportation/MapServer/legend) for the symbology of these features.

<img src="{{site.baseurl}}/assets/images/transportation.png" width="560" height="315">

### [NAIP](https://www.fsa.usda.gov/programs-and-services/aerial-photography/imagery-programs/naip-imagery/)

- **USGS NAIP Imagery**

- **NAIP Plus** - From the USDA: The National Agriculture Imagery Program (NAIP) acquires aerial imagery during the agricultural growing seasons in the continental U.S. A primary goal of the NAIP program is to make digital ortho photography available to governmental agencies and the public within a year of acquisition.

<img src="{{site.baseurl}}/assets/images/naip_plus.png" width="560" height="315">

- **ESA World Imagery** (not working)

### [Geology](https://www.sciencebase.gov/catalog/item/5888bf4fe4b05ccb964bab9d#:~:text=The%20SGMC%20is%20a%20compilation,have%20been%20%5B...%5D)

- **SGMC Geology** - From the USGS: The State Geologic Map Compilation (SGMC) geodatabase of the conterminous United States represents a seamless, spatial database of 48 State geologic maps that range from 1:50,000 to 1:1,000,000 scale. A national digital geologic map database is essential in interpreting other datasets that support numerous types of national-scale studies and assessments, such as those that provide geochemistry, remote sensing, or geophysical data. The SGMC is a compilation of the individual U.S. Geological Survey releases of the Preliminary Integrated Geologic Map Databases for the United States. The SGMC geodatabase also contains updated data for seven States and seven entirely new State geologic maps that have been added since the preliminary databases were published.

A full report on this dataset can be found [**here**](https://pubs.usgs.gov/ds/1052/ds1052.pdf).

<img src="{{site.baseurl}}/assets/images/sgmc.png" width="560" height="315">

![sgmc_geology]({{site.baseurl}}/assets/images/sgmc_legend.png)
