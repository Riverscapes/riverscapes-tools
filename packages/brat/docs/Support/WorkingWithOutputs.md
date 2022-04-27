---
title: Working with BRAT Outputs
weight: 3
---

The majority of users of BRAT will not actually run BRAT themselves, but instead will download BRAT outputs and summary products for use in beaver-related stream conservation and restoration efforts. In the text and videos tutorials below, we walk through various ways to interact with the BRAT outputs. We cover each of the outputs that BRAT produces, provide lookup tables for investigative purposes, and provide illustrative videos to help access and interrogate the outputs.

## The BRAT Default Symbology
The sections below describe the curated BRAT outputs that are visualized by default using the legends shown. The field within the attribute table that is used to symbolize the particular output is shown in parentheses after the output description.

![brat_capacity_output_example]({{site.baseurl }}/assets/images/capacity_example.png)

## Capacity Outputs
The **capacity** output layers include outputs that describe:
- existing capacity for beaver dam density (`oCC_EX` field)
- historic capacity for beaver dam density (`oCC_HPE` field)
- existing potential complex size (`mCC_EX_CT` field)
- historic potential complex size (`mCC_HPE_CT` field)

### Existing and Historic Capacity
The [**capacity model**]({{site.baseurl}}/Getting_Started/ModelLogic.html#capacity-estimates) produces two values for each stream segment: the density of dams that the segment can support now, and the density of dams that it could support historically. For display, these values are binned and symbolized using the color scheme below (note, while the outputs are binned for display, each ~300 m reach has a specific continuous dam density output). Within the attribute table of the stream network output, the existing capacity value is found in the `oCC_EX` field and historic capacity is found in the `oCC_HPE` field. These values represent modeled beaver dam capacity as dams per kilometer or mile for the stream network segment.

   ![Legend_BRAT_DamDensity]({{ site.baseurl }}/assets/images/Capacity_BRAT.png){: width="300" height="300"}

<div class="responsive-embed">
<iframe src="https://www.youtube.com/embed/odu5hHx2u9M" frameborder="0" allowfullscreen></iframe>
</div>

### Dam Complex Size
In addition, the capacity outputs and reach lengths were used to report estimated **dam complex size** `mCC_EX_CT` (for existing) and `mCC_HPE_CT` (for historic) fields in the attribute table. This is a modeled maximum number of the dams on that particular segment of the stream network. The following color scheme is used to illustrate these outputs:

   ![Legend_BRAT_DamComplex]({{ site.baseurl }}/assets/images/Dam_Complex_Size_BRAT.png){: width="300" height="300"}

## Management Outputs
The **management** output layers include outputs that describe:
- the limiting factors which contribute to unsuitable or limited beaver dam opportunities (`LimitationID` & `Limitation` fields in the attribute table)
- risk categories which are based on land use and anthropogenic proximity (`RiskID` & `Risk` fields)
- a measure of the effort exhibited to perform restoration or conservation in the segment (`OpportunityID` & `Opportunity` fields)

### Unsuitable or Limited Beaver Dam Opportunities
[(`Limitation`)]({{site.baseurl}}/Getting_Started/ModelLogic.html#unsuitable-or-limited-opportunities) Identifies areas where beaver cannot build dams now, and also differentiates stream segments into anthropogenically and naturally limited areas. The [logic for this output]({{site.baseurl}}/Getting_Started/ModelLogic.html#unsuitable-or-limited-opportunities) combines comparisons of existing and historic dam building capacity with slope and drainage area thresholds to determine potential limitations to dam building. The following color scheme is used to illustrate these distinctions:

(THIS LEGEND NEEDS TO BE UPDATED)
![Legend_BRAT_Management_Unsuitable_or_Limited_Beaver_Dam_Opportunities]({{ site.baseurl }}/assets/images/BRAT_Legends_Unsuitable.PNG){: width="350" height="350"}

<div class="responsive-embed">
<iframe src="https://www.youtube.com/embed/W8OCUairBT8" frameborder="0" allowfullscreen></iframe>
</div>

### Risk of Undesirable Dams
[(`Risk`)]({{site.baseurl}}/Getting_Started/ModelLogic.html#risk-of-undesirable-dams) provides a conservative estimate of risk of dam building activity to human infrastructure. The model calculates the distance from each stream segment to the nearest of various types of infrastructure (roads, railroads, canals, etc.), which are represented as geospatial inputs. This information on proximity to infrastructure is combined with dam building capacity outputs and land use in the riverscape surrounding each segment to estimate the potential risk that could result from dam building activity based on the [logic presented here]({{site.baseurl}}/Getting_Started/ModelLogic.html#risk-of-undesirable-dams).

(THIS LEGEND NEEDS TO BE UPDATED)
 ![Legend_BRAT Management Areas Beavers Can Build Dams, but Could Be Undesirable]({{ site.baseurl }}/assets/images/BRAT_Legends_07_2019_Risk.PNG){: width="300" height="300"}

<div class="responsive-embed">
<iframe src="https://www.youtube.com/embed/8PyrTsm9Yf0" frameborder="0" allowfullscreen></iframe>
</div>

### Restoration or Conservation Opportunities
[(`Opportunity`)]({{site.baseurl}}/Getting_Started/ModelLogic.html#restoration-or-conservation) identifies levels of effort required for establishing beaver dams on the landscape. This output is based on comparison between historic and existing dam building capacity, the risk of undesirable dams, and the land use in the riverscape surrounding stream segments based on [this model logic]({{site.baseurl}}/Getting_Started/ModelLogic.html#restoration-or-conservation).

(THIS LEGEND NEEDS TO BE UPDATED)
![Legend_BRAT_Management_Restoration_or_Conservation_Opportunities]({{ site.baseurl }}/assets/images/BRAT_Legend_06_2019_ConsRest.PNG){: width="300" height="300"}

<div class="responsive-embed">
<iframe src="https://www.youtube.com/embed/kYJJDzoGBnE" frameborder="0" allowfullscreen></iframe>
</div>

## The BRAT Attribute Table
This section lists and describes all of the attributes associated with network segments after BRAT has been run.
