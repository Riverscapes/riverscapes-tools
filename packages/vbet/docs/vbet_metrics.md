---
title: VBET Metrics
---

![vbet_areas](https://docs.google.com/drawings/d/e/2PACX-1vRZt634xFFKJ-EoN9hb9T1WnV77q-tByKwtiJk-k5mr7btqr-6R0Xzaq0tKG1EGIdv351kQgegcWlvc/pub?w=1040&h=714)

## LevelPath

Data Type|String
Units|NA
Description|The tributary identification provided by NHD Plus HR. All features along the longest path in a watershed possess the same level path attribute. Similarly with the next longest path down to its confluence with the longest path. see [NHD Value Added Attributes](https://www.usgs.gov/national-hydrography/value-added-attributes-vaas#LEVELPATHI)

## seg_distance

Data Type|Decimal
Units|Meters
Description|Distance from the outflow of the level path.

### stream_size

Data Type|Integer
Units|NA
Description|Stream Size Category for determining metric window size

## active_floodplain_area

Data Type|Decimal
Units|Square meters
Description|Total area of Active Floodplain within the analysis window

## active_floodplain_prop

Data Type|Decimal
Units|Ratio
Description|Proportion of Active Floodplain to analysis window area

## active_floodplain_area_cl_len

Data Type|Decimal
Units|Meters
Description|Active Floodplain Area per centerline length within the analysis window

## active_channel_area

Data Type|Decimal
Units|Square meters
Description|Total area of Active Channel within the analysis window

## active_channel_area_prop

Data Type|Decimal
Units|Ratio
Description|Proportion of Active Channel to analysis window area

## active_channel_area_cl_len

Data Type|Decimal
Units|Meters
Description|Active Channel Area per centerline length within the analysis window

## inactive_floodplain_area

Data Type|Decimal
Units|Square meters
Description|Total area of Inactive Floodplain within the analysis window

## inactive_floodplain_area_prop

Data Type|Decimal
Units|Ratio
Description|Proportion of Inactive Floodplain to analysis window area

## inactive_floodplain_area_cl_len

Data Type|Decimal
Units|Meters
Description|Inactive Floodplain Area per centerline length within the analysis window

## floodplain_area

Data Type|Decimal
Units|Square meters
Description|Total area of Floodplain within the analysis window

## floodplain_area_prop

Data Type|Decimal
Units|Ratio
Description|Proportion of Floodplain to analysis window area

## floodplain_area_cl_len

Data Type|Decimal
Units|Meters
Description|Floodplain Area per centerline length within the analysis window

## integrated_width

Data Type|Decimal
Units|Meters
Description|Integerated Width calculated as window_area / centerline_length

## window_size

Data Type|Decimal
Units| Square Meters
Description|Target Length of analysis window

## window_area

Data Type|Decimal
Units|Square meters
Description|Total area of window

## centerline_length

Data Type|Decimal
Units|Meters
Description|Total length of centerline clipped to window
