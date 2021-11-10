---
title: Working with BRAT Outputs
weight: 2
---

The majority of users of BRAT will not actually run BRAT themselves, but instead will download BRAT outputs and summary products for use in beaver-related stream conservation and restoration efforts. In the text and videos tutorials below, we walk through various ways to interact with the BRAT outputs. We cover each of the outputs that BRAT produces, provide lookup tables for investigative purposes, and provide illustrative videos to help access and interrogate the outputs. 

## The BRAT Default Symbology
The sections below describe the curated BRAT outputs that are visualized by default using the legends shown. The field within the attribute table that is used to symbolize the particular output is shown in parentheses after the output description.

![brat_capacity_output_example]({{site.baseurl }}/assets/images/capacity_example.png)

### Capacity Outputs
The **capacity** output layers include outputs that describe:
- existing capacity for beaver dam density (`oCC_EX` field)
- historic capacity for beaver dam density (`0CC_HPE` field)
- existing potential complex size (`mCC_EX_CT` field)
- historic potential complex size (`mCC_HPE_CT` field)

#### Existing and Historic Capacity 
The **capacity model** outputs use the following color scheme to bin the output data (note each ≤300 m reach has a specific continuous dam density output). Existing capacity output is found in the `oCC_EX` field and historic capacity is found in the `oCC_HPE` field. These values represent modeled beaver dam capacity as dams per kilometer or mile of a particular segment within the stream network. The following color scheme is used to illustrate these outputs:

   ![Legend_BRAT_DamDensity]({{ site.baseurl }}/assets/images/Capacity_BRAT.png){: width="300" height="300"}

#### Dam Complex Size
In addition, the capacity outputs and reach lengths were used to report estimated **dam complex size** `mCC_EX_CT` (for existing) and `mCC_HPE_CT` (for historic) fields. This is a modeled maximum number of the dams on that particular segment of the stream network. The following color scheme is used to illustrate these outputs:

   ![Legend_BRAT_DamComplex]({{ site.baseurl }}/assets/images/Dam_Complex_Size_BRAT.png){: width="300" height="300"}

### Management Outputs
The **management** output layers include outputs that describe: 
- the limiting factors which contribute to unsuitable or limited beaver dam opportunites (`LimitationID` & `Limitation` fields)
- risk categories which are based on land use and antrhopogenic proximity (`RiskID` & `Risk` fields)
- a measure of the effort exhibited to perform restoration or conservation in the segment (`OpportunityID` & `Opportunity` fields)

#### Unsuitable or Limited Beaver Dam Opportunities
(`Limitation`) Identifies areas where beaver cannot build dams now, and also differentiates stream segments into antrhopogenically and naturally limited areas. The following color scheme is used to illustrate these distinctions:

![Legend_BRAT_Management_Unsuitable_or_Limited_Beaver_Dam_Opportunities]({{ site.baseurl }}/assets/images/BRAT_Legends_Unsuitable.PNG){: width="350" height="350"}

#### Risk of Undesirable Dams
(`Risk`) Identifies riverscapes that are close to human infrastructure or high land use intensity and where the capacity model estimates that beavers can build dams. 

 ![Legend_BRAT Management Areas Beavers Can Build Dams, but Could Be Undesirable]({{ site.baseurl }}/assets/images/BRAT_Legends_07_2019_Risk.PNG){: width="300" height="300"}

#### Restoration or Conservation Opportunities
(`Opportunity`) Identifies levels of effort required for establishing beaver dams on the landscape.

![Legend_BRAT_Management_Restoration_or_Conservation_Opportunities]({{ site.baseurl }}/assets/images/BRAT_Legend_06_2019_ConsRest.PNG){: width="300" height="300"}

## The BRAT Attribute Table
This section lists and describes all of the attributes associated with network segments after BRAT has been run.

