---
title: Application with NHD
weight: 2
---

The current [version](https://github.com/Riverscapes/riverscapes-tools/tree/master/packages/channel) of the tool is often driven using the free, nationally available [National Hydrography Dataset (NHD)](https://www.usgs.gov/core-science-systems/ngp/national-hydrography/national-hydrography-dataset?qt-science_support_page_related_con=0#qt-science_support_page_related_con), and is therefore applicable to U.S. Riverscapes projects. The NHD dataset includes three layers that are used as tool inputs:
- NHD Flowlines, are lines that represent the channel network. They include attributes that identify flow type (e.g., perennial, intermittent, canal)
- NHD Area are polygons representing river channels for larger rivers. These are digitized from USGS 1:24000 topographic quadrangles. Lines from the Flowlines layer pass through these polygons and are coded as "artificial paths".
- NHD Waterbodies are polygons representing lakes ponds and reservoirs, and wetlands.

The Channel Area Tool subsets these layers, keeping "Area" and "Waterbody" polygons that have "artificial path" flowlines passing through them, indicating that they are connected to and part of the drainage network. In the remainder of the network, for which there are no polygons, an empirical equation e.g., [Beechie & Imaki (2014)](https://agupubs.onlinelibrary.wiley.com/doi/full/10.1002/2013WR013629) is used to estimate channel width from drainage area, and this value is used to buffer the network segments. Finally the filtered "Area" and "Waterbody" polygons are merged with the buffers to create a final channel area output.

<div class="responsive-embed">
<iframe width="560" height="315" src="https://www.youtube.com/embed/5wcYy3UwC-s" frameborder="0" allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</div>
