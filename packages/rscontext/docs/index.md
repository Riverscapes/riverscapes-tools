---
title: Home
---

## About
The **Riverscape Context Tool** is a tool that aggregates contextual layers for consumption in other Riverscapes projects. Many Riverscapes tools use nationally available datasets that have to be retrieved from various sources and prepared for an area of interest. This process can be time consuming, and often the data have to be further processed after retrieving them (for example, mosaic of DEM tiles, clipping to watershed boundaries, etc.), and these processes can introduce problems into the datasets. This tool resolves these potential issues by retrieving these datasets, processing them, and organizing them within a Riverscapes project automatically. These data can then be used on their own, or as inputs to other tools.

The geospatial layers that the tool collects are:
- **Topography** (Digital Elevation Models) from the [National Elevation Dataset (NED)](https://gdg.sc.egov.usda.gov/Catalog/ProductDescription/NED.html), from which additional layers are derived:
  - Slope
  - Flow Accumulation
  - Drainage area
  - Detrended DEM
  - Hillshades for context
- [**LANDFIRE**](https://landfire.gov/) vegetation:
  - Existing vegetation (class, name) from which an existing riparian layer is derived
  - Historic vegetation (name) from which a historic riparian layer is derived
- **Land Management**:
  - Land ownership/agency from [BLM Surface Management Agency](https://gbp-blm-egis.hub.arcgis.com/datasets/6bf2e737c59d4111be92420ee5ab0b46/about)
  - Fair market value from [PLACES Lab](https://placeslab.org/fmv_usa/), Department of Earth & Environment, Boston University
- **Ecoregions**:
  - level 1, 2, and 3 Ecoregions from the [EPA](https://www.epa.gov/eco-research/ecoregions)
- **Climate** [(PRISM)](https://prism.oregonstate.edu/):
  - Mean Annual Precipitation
  - Mean Annual Temperature
  - Minimum Temperature
  - Maximum Temperature
  - Mean Dewpoint Temperature
  - Minimum Vapor Pressure Deficit
  - Maximum Vapor Pressure Deficit
- **Hydrology**:
  - Hydrography ([NHD HR+](https://www.usgs.gov/core-science-systems/ngp/national-hydrography/nhdplus-high-resolution))
  - Watershed boundaries (also from NHD)
- **Transportation**:
  - Roads from [TIGER](https://data-usdot.opendata.arcgis.com/documents/usdot::census-tiger-line-roads/about)
  - Railroads

<div class="responsive-embed">
<iframe width="560" height="315" src="https://www.youtube.com/embed/1Qp8CpGmaAE" frameborder="0" allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</div>
