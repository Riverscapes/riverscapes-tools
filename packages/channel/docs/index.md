---
title: Home
---

## About
The **The Channel Area Tool** is a simple tool for generating polygons representing the spatial extent of the drainage network within a watershed. The primary purpose for the tool is that the outputs it produces are used as *inputs* in other Riverscapes tools. Geospatial tools often use a simple line network to represent streams. Depending on the functions a tool is performing, this can be problematic as a line can represent both a narrow, first order stream as well as large, wide rivers. Many Riverscapes tools analyze areas outside of the channel (for example, to look at streamside vegetation), therefore an accurate representation of the actual channel, not simply a line, is necessary. The tool is comprised of a simple algorithm for combining polygons representing channels with polygons derived from attributes on a drainage network (line). As long as a drainage network has an attribute recording the upstream contributing drainage area for each segment, regional relationships relating channel width to drainage area can be used to buffer the channel segments, and the resulting polygons can be merged with any other available polygons. This gives a first order approximation of the active channel area. As channels are active and constantly moving through time, greater accuracy can be achieved with more recent, high resolution datasets, or with user input (e.g., editing channel positions or channel polygons). 

![channelarea]({{site.baseurl}}/assets/images/chan_area.png)
