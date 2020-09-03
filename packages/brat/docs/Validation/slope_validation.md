---
title: Slope
---

I ran both the [GDAL slope](https://gdal.org/programs/gdaldem.html#slope) and [ArcGIS slope](https://pro.arcgis.com/en/pro-app/tool-reference/3d-analyst/how-slope-works.htm) routines on Sulphur Creek 2005 DEM to assess whether they would produce the same result. The slope rasters were identical to the fourth decimal place.

I then took one of the NED 10m DEMs and ran it through both the ESRI and GDAL slope routines. Note that this was done in the original geographic coordinate system without resampling. In GDAL I used the generic scale factor of 111120 [described on the documentation](https://gdal.org/programs/gdaldem.html#slope). ESRI allows for the same conversion factor but in this case it's vertical/horizontal units, so I used the reciprocal of 0.000008999280058.

<a href ="{{ site.baseurl }}/assets/images/validation/ned_slope_analysis.png"><img src="{{ site.baseurl }}/assets/images/validation/ned_slope_analysis.png"></a>

The differences in the two slope routines were down to the third or fourth decimal place of a degree! This makes sense when you consider that both algorithms use a 3x3 kernal and a planar algorithm approach.

## Resources

* [GDAL Slope](https://gdal.org/programs/gdaldem.html)
* [ESRI Slope](https://pro.arcgis.com/en/pro-app/tool-reference/3d-analyst/how-slope-works.htm)
* [Haversine forumla](https://rosettacode.org/wiki/Haversine_formula#Python)
* [ESRI blog post](https://www.esri.com/arcgis-blog/products/product/imagery/setting-the-z-factor-parameter-correctly/) that explains why vertical scaling factor is required when calculating slope on rasters with geographic coordinate systems.
