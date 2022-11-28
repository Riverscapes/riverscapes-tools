---
title: BRAT Reach Attributes Table
---

BRAT stores input, intermediate and output data in a GeoPackage. The two main [database tables in this GeoPackage](Advanced_users/database.html) are the `Reaches` and `ReachAttributes`. The polyline geometry for each reach is stored in the `Reaches` table. This "feature class" is then related to the `ReachAttributes` table that contains the numerous attributes for each feature needed to run BRAT.

Each column in the `ReachAttributes` table is described below. Columns are grouped into two categories depending on which [model phase](Advanced_Users/architecture.html) they are populated; input and intermediate attributes that are calculated once during the BRAT Build phase of running the tool, and output attributes that are calculated each time that the BRAT Run phase of the model is run.

## Input & Intermediate Attributes

### ReachID

Data Type|Integer
Units|Auto-incrementing ID
Description|Automatically asigned unique identifier of each reach. This is the primary key (FID, OID) for each feature.

### WatershedID

Data Type|Text
Units|NA
Description|Identifies which watershed the reach belongs to. This value references the `Watersheds` table. For North American BRAT runs this typically represents the HUC8 identifier.

### Geometry

Data Type|Binary
Units|NA
Description|The Reach polyline Geometry.

### ReachCode

Data Type|Integer
Units|NA
Description|Identifies the type of reach. This column references the ReachCodes defined in the `ReachCodes` table. For North American runs this is typically the NHD `FCode`.

### IsPeren

Data Type|Integer
Units|Boolean
Description|Depicts whether the reach is perennial or not. A value of zero means that the reach is NOT perennial. Any other value means that the reach is perennial. This column cannot be NULL.

### IsMainCh

Data Type|Ingeger
Units|Boolean
Description|Indicates whether the channel is the mainstem or not. Zero indicates that the reach is a side channel, diversion or other anabranch. Any other value indicates that the channel is the mainstem.

### IsMultiCh

Data Type|Integer
Units|Boolean
Description|Indicates whether the reach polyline represents a multi-threaded channel or not. Zero indicates that the reach is single threaded. Any other value indicates that the feature represents a multi-threaded channel.

### StreamName

Data Type|Text
Units|NA
Description|Optional stream name. For North American BRAT runs this typically represents the GNIS feature name.


### iGeo_Slope

Data Type|Float
Units|Percent
Description|Slope of the reach flow line. When BRAT calculates this value is samples the DEM at both ends of the reach polyline using a 30m buffer. The average elevation in each circle is subtracted and the difference divided by the length of the reach polyline.

### iGeo_ElMax

Data Type|Float
Units|Meters
Description|The higher elevation of either end of the reach polyline.  When BRAT calculates this value it samples the DEM at both ends of the reach polyline using a 30m buffer. The higher of the two averages is then used as the max elevation.

### iGeo_ElMin

Data Type|Float
Units|Meters
Description|The lower elevation of either end of the reach polyline.  When BRAT calculates this value it samples the DEM at both ends of the reach polyline using a 30m buffer. The lower of the two averages is then used as the max elevation.

### iGeo_Len

Data Type|Float
Units|Meters
Description|Length of the reach flow line polyline. When BRAT calculates this attribute is measures the actual, sinuous length along the polyline.

### Orig_DA

Data Type|Float
Units|Square kilometers. 
Description|The total upstream drainage of the area that flows into the reach. This column differs from `iGeo_DA` because this value is never changed from the original data. It is provided as a means of comparing where users have overidden the original drainage area values. For North American BRAT runs this value is populated from the NHDPlus HR `totDASqKm` VAA attribute.

### iGeo_DA

Data Type|Float
Units|Square Kilometers
Description|This is the actual upstream drainage area used by BRAT when calculating the hydrology for a reach. This is initiated as a copy of the `Orig_DA` attribute value, but can be overridden should that value be deemed inaccurate.

### AgencyID

Data Type|Integer
Units|NA
Description|Identifies the land ownership for the property underlying the reach. This is calculated by intersecting the feature polyline with the land ownership layer and determining the agency that possessess the greatest length of reach. The ID is a lookup on the `Agencies` database table.


## Output Attributes

### iVeg100EX

Data Type|Real
Units|Beaver dams per kilometer
Description|Existing beaver dam capacity based solely on vegetation located within 100m of the stream.

### iVeg_30EX

Data Type|Real
Units|Beaver dams per kilometer
Description|Existing beaver dam capacity based solely on vegetation located within 30m of the stream.

### iVeg100HPE

Data Type|Real
Units|Beaver dams per kilometer
Description|Historic beaver dam capacity based solely on vegetation located within 100m of the stream.

### iVeg_30HPE

Data Type|Real
Units|Beaver dams per kilometer
Description|Historic beaver dam capacity based solely on vegetation located within 100m of the stream.

### iPC_Road

Data Type|Real
Units|
Description|

### iPC_RoadX

### iPC_RoadVB

Data Type|Real
Units|
Description|


### iPC_Rail

Data Type|Real
Units|
Description|

### iPC_RailVB

Data Type|Real
Units|
Description|

### iPC_LU

Data Type|Real
Units|
Description|

### iPC_VLowLU

Data Type|Real
Units|
Description|

### iPC_LowLU

Data Type|Real
Units|
Description|

### iPC_ModLU

Data Type|Real
Units|
Description|

### iPC_HighLU

Data Type|Real
Units|
Description|

### iHyd_QLow

Data Type|Real
Units|Cubic feet per second (CFS)
Description|Base streamflow that is execeeded 90% of the time. Low flow conditions are used in BRAT to determine if there is emough streamflow to support ponding behind beaver dams.

#### iHyd_Q2

Data Type|Real
Units|Cubic feet per second (CFS)
Description|High streamflow that reccurs every two years. This flood discharge is used to calculate when high flows will cause dam blowouts.

### iHyd_SPLow

Data Type|Real
Units|
Description|Base stream power is caculated by converting the base streamflow (`iHyd_QLow`) into stream power using the discharge by the slope and the gravitational constant.

### iHyd_SP2

Data Type|Real
Units|
Description|High flow stream power is caculated by converting the base streamflow (`iHyd_Q2`) into stream power using the discharge by the slope and the gravitational constant.

### oVC_HPE

Data Type|Real
Units|
Description|

### oVC_EX

Data Type|Real
Units|
Description|


### oCC_HPE

Data Type|Real
Units|
Description|

### mCC_HPE_CT

Data Type|Real
Units|Beaver Dam Count
Description|The historic number of beaver dams that the reach could support. This is calculaterd by multiplying the historic beaver dam capacity (`oHPE_EX`) by the length of the channel (`iGeo_Len`).

### oCC_EX

Data Type|Real
Units|Beaver Dams Per Kilometer
Description|Existing Beaver Dam Capacity. This is the main output of the beaver dam fuzzy inference system.

### mCC_EX_CT

Data Type|Real
Units|Beaver Dam Count
Description|

### LimitationID

Data Type|Integer
Units|NA
Description|The primary limitations to beaver dams occurring on the reach. This is a lookup to the `Limitations` table in the BRAT database. 

### RiskID

Data Type|Integer
Units|NA
Description|The primary risk to beaver dams occurring on the reach. This is a lookup to the `Risk` table in the BRAT database.

### OpportunityID

Data Type|Integer
Units|NA
Description|The types of opportunities for beaver dams occurring on the reach. This is a lookup to the `Opportunities` table in the BRAT database.

### ManagementID

Data Type|Integer
Units|
Description|

### iPC_Canal

Data Type|Real
Units|
Description|

### iPC_DivPts

Data Type|Real
Units|
Description|

### iPC_Privat

Data Type|Real
Units|
Description|

### oPC_Dist

Data Type|Real
Units|
Description|

### mCC_HisDep

Data Type|Real
Units|
Description|
