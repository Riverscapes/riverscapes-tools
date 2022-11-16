---
title: Channel Area Data
weight: 2
---

<h2><a name="FILTERED_WATERBODY">Filtered NHD Waterbodies</a></h2>

A subset of the NHDWaterbody layer filtered using specified NHD FCodes.

<h2><a name="FILTERED_FLOWAREAS">Filtered NHD Areas</a></h2>

A subset of the NHDArea layer filtered using specified NHD FCodes.

<h2><a name="COMBINED_FA_WB">NHD Waterbodies and NHD Areas combined</a></h2>

The NHDArea and NHDWaterbody layers combined.

<h2><a name="BANKFULL_NETWORK">Bankfull Network</a></h2>

The NHDFlowlines layer with an attribute 'bankfull_m' added that is used to buffer the network. This layer is the network without the segments that fall within NHDWaterbody or NHDArea features.

<h2><a name="BANKFULL_POLYGONS">Bankfull Polygons</a></h2>

Polygons derived by buffering the bankfull_network feature using the 'bankfull_m' attribute. This value is calculated using region equations for bankfull width.

<h2><a name="DIFFERENCE_POLYGONS">Difference Polygons</a></h2>

The bankfull polygons with areas that overlap the NHDArea layer removed.

<h2><a name="CHANNEL_AREA">Channel Area Polygons</a></h2>

A polygon feature class representing the bankfull channel for the input network. The filtered NHDArea and NHDWaterbody features are combined with the buffered NHDFlowline features to create a single feature class.