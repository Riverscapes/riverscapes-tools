---
title: Algorithm
---

This temporary page describes the current experimental VBET algorithm.

### Step 1 - Slope Evidence

The first step is to divide the original slope raster by -12 and then add 1. We then set any negative values to NULL. ([code](https://github.com/Riverscapes/sqlBRAT/blob/master/vbet.py#L144-L145)). The constant value of 12 is passed to the script as an argument so that it can be adjusted easily.

The goal is to invert the raster so that low slope values have the highest raster values and then discard (set null) any areas that had an original slope value of 12 degrees or greater. The result is a raster with the following range

|Original Slope Value|Output Value|
|---|---|
|0|1|
|12|0|
|> 12|NULL|
|NULL|NULL|

### Step 2 - HAND Evidence

The HAND raster is treated in the same away as the slope raster. It is divided by -50 and then 1 added to the result. ([code](https://github.com/Riverscapes/sqlBRAT/blob/master/vbet.py#L148-L149)). Again the constant value of 50 is passed as an argument.

The goal is to invert the raster so that high HAND values have the lowest raster values and then discard and values greater than 50 m.

|Original HAND Value|Output Value|
|---|---|
|0|1|
|50|0|
|> 50|NULL|
|NULL|NULL|

### Step 3 - Channel Evidence

A third evidence raster is generated that represents wet areas - essentially areas virtually guaranteed to be valley bottom. First the perennial polyline network is buffered by 25 metres ([code](https://github.com/Riverscapes/sqlBRAT/blob/master/vbet.py#L92)) and converted into a raster. Next the flow area polygons representing large channels are also rasterized and combined into the network raster. 

|Feature|Output Value|
|Perennial network buffered by 25 m| 1|
|Flow area polygons| 1|
|All other areas|Null|

### Step 4  - Combined Evidence 

The slope evidence and HAND evidence rasters are multiplied together ([code](https://github.com/Riverscapes/sqlBRAT/blob/master/vbet.py#L150)). The channel evidence is then burned into the resultant raster. Essentially the result of the multiplication is updated to be 1 wherever the channel evidence raster is 1.

The end result is a raster with a value of 1 in the channel and flow areas. Just less than 1 in flat areas at elevations close to the channel. Zero occurs in steep areas or places at higher elevations above the channel.

### Step 5 - Thershold and Vectorize

The combined evidence raster is then thresholded in increments of 0.1 from 0.5 to 1.0 ([code](https://github.com/Riverscapes/sqlBRAT/blob/master/vbet.py#L180)). At each step values less than the threshold are set to NULL. The resultant raster is then vectorized and written to a series of ShapeFiles.

### Step 6 - Polygon Sanitation

The polygons from the previous step are sanitized ([code](https://github.com/Riverscapes/sqlBRAT/blob/master/vbet.py#L231-L280)) and written to the final output Shapefiles.

* Polygons are simplified
* Small donuts are removed
* Negatively buffered
* Small holes are removed
* All polygons unioned together

### VBET Project

Here's a description of the final VBET project folder structure:

|Path|Description|
|---|---|
|inputs/channel.tif|25 m buffer around perennial network converted to a raster.|
|inputs/dem_hillshade.tif|context only. Not used in analysis.|
|inputs/flow_areas.shp|Original NHD flow area polygons|
|inputs/flowlines.shp|Original NHD network polygons|
|inputs/hand.tif|Original HAND raster|
|inputs/slope.tif|Original slope raster in degress|
|intermediates/Evidence.tif|Multiband evidence raster. Band 1 is slope evidence multiplied by HAND evidence. Band 2 is channel evidence.|
|intermediates/thresh_50 to _100.shp|Combined evidence raster thresholded at increments of 0.1 converted to vector. No sanitization.|
|intermediates/vbet_network.shp|filtered flow line network. Currently just perennial.|
|outputs/vbet_68 to _100.shp|Final sanitized VBET polygons in increments of 0.1 probability of being valley bottom.|
|project.rs.xml|Riverscapes project file|
|vbet.log|Processing log|


