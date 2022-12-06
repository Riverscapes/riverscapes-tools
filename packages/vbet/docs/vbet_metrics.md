Data Type|Decimal
---
title: VBET Metrics
---

![vbet_areas](https://docs.google.com/drawings/d/e/2PACX-1vRZt634xFFKJ-EoN9hb9T1WnV77q-tByKwtiJk-k5mr7btqr-6R0Xzaq0tKG1EGIdv351kQgegcWlvc/pub?w=1040&h=714)

## LevelPathI

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

## active_floodplain_proportion

Data Type|Decimal
Units|Ratio
Description|Proportion of Active Floodplain area to analysis window area

## active_floodplain_area_itgr_width

Data Type|Decimal
Units|Meters
Description|Active Floodplain Area divided by centerline length within the analysis window

## active_channel_area

Data Type|Decimal
Units|Square meters
Description|Total area of Active Channel within the analysis window

## active_channel_area_proportion

Data Type|Decimal
Units|Ratio
Description|Proportion of Active Channel area to analysis window area

## active_channel_area_itgr_width

Data Type|Decimal
Units|Meters
Description|Active Channel Area divided by centerline length within the analysis window

## inactive_floodplain_area

Data Type|Decimal
Units|Square meters
Description|Total area of Inactive Floodplain within the analysis window

## inactive_floodplain_area_proportion

Data Type|Decimal
Units|Ratio
Description|Proportion of Inactive Floodplain area to analysis window area

## inactive_floodplain_area_itgr_width

Data Type|Decimal
Units|Meters
Description|Inactive Floodplain Area divided by centerline length within the analysis window

## floodplain_area

Data Type|Decimal
Units|Square meters
Description|Total area of Floodplain (Active + Inactive) within the analysis window

## floodplain_area_proportion

Data Type|Decimal
Units|Ratio
Description|Proportion of Floodplain to analysis window area

## floodplain_area_itgr_width

Data Type|Decimal
Units|Meters
Description|Floodplain Area divided by centerline length within the analysis window

## integrated_width

Data Type|Decimal
Units|Meters
Description|Integerated Width calculated as window_area / centerline_length

## vb_acreage_per_mile

Data Type|Decimal
Units|acres/mile
Description|window_area converted to acres and divided by centerline_length converted to miles.

## vb_hectares_per_km

Data Type|Decimal
Units|hectares/km
Description|window_area converted to hectares and divided by centerline_length converted to kilometers

## active_acreage_per_mile

Data Type|Decimal
Units|acres/mile
Description|active_floodplain_area plus active_channel_area converted to acres and divided by centerline_length converted to miles

## active_hectares_per_km

Data Type|Decimal
Units|hectares/km
Description|active_floodplain_area plus active_channel_area converted to hectares and divided by centerline_length converted to kilometers

## inactive_acreage_per_mile

Data Type|Decimal
Units|acres/mile
Description|inactive_floodplain_area converted to acres and divided by centerline_length converted to miles

## inactive_hectares_per_km

Data Type|Decimal
Units|hectares/km
Description|inactive_floodplain_area converted to hectares and divided by centerline_length converted to kilometers

## window_size

Data Type|Decimal
Units|Meters
Description|Target Length of analysis window

## window_area

Data Type|Decimal
Units|Square meters
Description|Total area of window

## centerline_length

Data Type|Decimal
Units|Meters
Description|Total length of centerline clipped to window
