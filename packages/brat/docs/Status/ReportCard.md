---
title: Riverscapes Report Card
weight: 1
---

BRAT is one of several tools developed by the [Riverscapes Consortium](https://riverscapes.xyz). This report card communicates BRAT's compliance with the Riverscape Consortium's published [tool standards](https://riverscapes.xyz/Tools).

# Report Card Summary

| Tool | [BRAT - Beaver Restoration Assessment Tool](https://tools.riverscapes.xyz/brat) |
| Version | [4.3.2](https://github.com/Riverscapes/riverscapes-tools/tree/master/packages/brat) (experimental, not an official release) |
| Date | 2021-11-22 |
| Assessment Team | Bailey, Wheaton & Gilbert |
| Current Assessment | ![production](https://raw.githubusercontent.com/Riverscapes/riverscapes-website/master/assets/images/tools/grade/TRL_5_32p.png) Production Grade |
| Target Status | ![commercial](https://raw.githubusercontent.com/Riverscapes/riverscapes-website/master/assets/images/tools/grade/TRL_7_32p.png) Commercial Grade |
| Riverscapes Compliance | ![Compliant](https://riverscapes.xyz/assets/images/rc/RiverscapesCompliant_24.png) Compliant|
| Assessment Rationale | BRAT has been applied extensively throughout the Western US and in the UK. It has been used extensively to inform policy and planning and state-wide, regional and watershed extents, but also to inform restoration planning and design at the reach-scale. Others have applied the model, but for the most part it has been implemented by the USU ETAL team. It's wide use, scalability, and documentation result in a Production Grade. |


# Report Card Details

This tool's [discrimination](https://riverscapes.xyz/Tools/#model-discrimination) evaluation by the [Riverscapes Consortium's](https://riverscapes.xyz) is:

**Evaluation Key:**
None or Not Applicable: <i class="fa fa-battery-empty" aria-hidden="true"></i> •
Minimal or In Progress: <i class="fa fa-battery-quarter" aria-hidden="true"></i> •
Functional: <i class="fa fa-battery-half" aria-hidden="true"></i> •
Fully Developed: <i class="fa fa-battery-full" aria-hidden="true"></i>  

| Criteria | Value | Evaluation | Comments and/or Recommendations |
|----------|-------|------------|---------------------------------|
| :------- | :---- | :--------- | :------------------------------ |
| Tool Interface(s) | <i class="fa fa-terminal" aria-hidden="true"></i> : CLI = [Command Line Interface](https://en.wikipedia.org/wiki/Command-line_interface) | <i class="fa fa-battery-full" aria-hidden="true"></i> | Tool is two primary command line commands that specify inputs and use lookup tables for parameterization. The tables contain information specific to freely available US data, but can be extended to contain other data. |
| Scale | Network (reach scale resolution, watershed extent) | <i class="fa fa-battery-full" aria-hidden="true"></i> | This tool has been applied across entire states, regions and watersheds, resolving detail down to 250 m to 300 m length reaches of riverscape. |
| Language(s) and Dependencies | Python | <i class="fa fa-battery-full" aria-hidden="true"></i> | Python package dependencies are open source. |
| Vetted in Peer-Reviewed Literature | Yes.  [Macfarlane et al. (2015)]({{site.baseurl}}/references.html) | <i class="fa fa-battery-half" aria-hidden="true"></i> | The existing capacity model is vetted, and the historical capacity model is well described. The version in the publication is [2.0](https://github.com/Riverscapes/pyBRAT/releases/tag/v2.0.0), but the capacity model is basically the same in 3.0. Many of the risk, beaver management, conservation and restoration concepts have not yet been vetted in scholarly literature but have been applied, tested and vetted by many scientists and managers across the US and UK. |
| Source Code Documentation | Available at [Github](https://github.com/Riverscapes/riverscaps-tools/brat) <i class="fa fa-github" aria-hidden="true"></i> | <i class="fa fa-battery-full" aria-hidden="true"></i> | Source code is clearly organized and documented |
| Open Source | [open-source](https://github.com/Riverscapes/riverscaps-tools/brat) <i class="fa fa-github" aria-hidden="true"></i> with [GNU General Public License v 3.0](https://github.com/Riverscapes/riverscapes-tools/blob/master/LICENSE) | <i class="fa fa-battery-full" aria-hidden="true"></i> | |
| User Documentation | [Documentation](http://tools.riverscapes.xyz/brat/) is in progress. | <i class="fa fa-battery-half" aria-hidden="true"></i> | Previous versions were thoroughly documented but much of that documentation is out of date with the current version. New, updated documentation for sqlBRAT is currently in progress. |
| Easy User Interface | Tool is accessed via command prompt. | <i class="fa fa-battery-quarter" aria-hidden="true"></i> | Most BRAT runs right now are performed by North Arrow Research analysts. For a user to run this version of the tool themselves would require advanced understanding (programming capability) and significant documentation on the process. |
| Scalability | Batch processing | <i class="fa fa-battery-full" aria-hidden="true"></i> | The tool is typically batch processed at the HUC 8 level |
| Produces Riverscapes Projects <img  src="https://riverscapes.xyz/assets/images/data/RiverscapesProject_24.png"> | Yes | <i class="fa fa-battery-half" aria-hidden="true"></i> | BRAT is now producing Riverscapes Projects that are fully-compatible with [RAVE](https://rave.riverscapes.xyz), but some changes to curation and business logic are still being made |

## Tool Output Utility

| Criteria | Value | Evaluation | Comments |
|----------|-------|------------|----------|
| :------- | :---- | :--------- | :------- |
| [RAVE](https://rave.riverscapes.xyz)- Compliant Riverscapes Projects <img  src="https://riverscapes.xyz/assets/images/data/RiverscapesProject_24.png"> | Yes | <i class="fa fa-battery-full" aria-hidden="true"></i> | |
| [RAVE](https://rave.riverscapes.xyz) Business Logic Defined | Yes | <i class="fa fa-battery-half" aria-hidden="true"></i> | Business logic is defined, but undergoing minor changes and not finalized for a release |
| Riverscapes Projects hosted in public-facing [Riverscapes Warehouse(s)](https://riverscapes.xyz/Data_Warehouses/#warehouse-explorer-concept) <img src="https://riverscapes.xyz/assets/images/data/RiverscapesWarehouseCloud_24.png"> | Yes. Current BRAT runs are being housed in the [Riverscapes Warehouse](https://data.riverscapes.xyz). | <i class="fa fa-battery-half" aria-hidden="true"></i> | There is data in the warehouse, but is provisional and wider access to the warehouse is forthcoming |
| Riverscapes Projects connected to [Web-Maps](https://riverscapes.xyz/Data_Warehouses#web-maps) <i class="fa fa-map-o" aria-hidden="true"></i> | Current projects are viewable in [WebRAVE](https://rave.riverscapes.xyz/Download/install_webrave.html) through the Riverscapes Warehouse. A [DataBasin](https://databasin.org/datasets/1420ffb7e9674753a5fb626e2b830c1f) entry exists for Utah BRAT | <i class="fa fa-battery-full" aria-hidden="true"></i> | All old data sets should be made Web Map accessible and clear about what version they were produced from and what years they correspond to (i.e. Riverscapes Project metadata) |
| Riverscapes Projects connected to Field [Apps](https://riverscapes.xyz//Data_Warehouses#apps---pwas) <img src="http://riverscapes.xyz/assets/images/tools/PWA.png"> | Not publicly. Some simple Arc Data Collector field apps have been used, but they are not reliable, scalable or deployable to external audiences. | <i class="fa fa-battery-quarter" aria-hidden="true"></i> | Workflows and forms are well tested and vetted. This needs funding to develop as commercial, professional-grade reliable web app. |

## Developer Intent
The current (unreleased) **Production Grade** <img  src="https://riverscapes.xyz/assets/images/tools/grade/TRL_6_32p.png"> version of BRAT will improve on previous versions by:
- Having an inviting [web-map interface](https://riverscapes.xyz/Data_Warehouses/#web-maps) so non GIS-users can discover BRAT runs and explore them and interrogate them.
- Making it easy for GIS users to download BRAT for use in [RAVE](https://rave.riverscapes.xyz) with [Riverscapes Projects](https://riverscapes.xyz/Tools/Technical_Reference/Documentation_Standards/Riverscapes_Projects/) <img  src="https://riverscapes.xyz/assets/images/data/RiverscapesProject_24.png">
- Allowing discovery of past BRAT runs in Warehouse
- Presenting transparent ranking of level of BRAT model curation or [dataset rank](https://riverscapes.xyz/Data_Warehouses/#dataset-rank) and facilitating community commenting

The BRAT development team are in the process of building a **Commercial Grade** <img src="https://riverscapes.xyz/assets/images/tools/grade/TRL_7_32p.png"> version of BRAT, which would additionally:
- Encourage more user-interaction with BRAT outputs and crowd-sourcing of information to create ownership of outputs
- Allow users to visualize dynamic outputs of BRAT through time
- Allow users to upload their own BRAT projects
- Allow users to provide their own inputs locally (@ a reach) and produce local realizations.
- Allow users to upload (or make) their own beaver dam and activity observations
- Facilitate users paying modest prices (i.e. commercial) to have new runs or more carefully curated (validated, resolved, etc.) for a specific watershed and then share them with broader community

This report card is for a current, beta version of a **Production Grade** <img  src="https://riverscapes.xyz/assets/images/tools/grade/TRL_6_32p.png"> version of BRAT ([sqlBRAT](https://github.com/Riverscapes/riverscapes-tools/brat) with no release yet), which will be necessary to support the **Commercial Grade**  <img src="https://riverscapes.xyz/assets/images/tools/grade/TRL_7_32p.png"> product.

If you share this [vision]({{ site.baseurl }}/Vision.html), get in touch with the developers to support/fund the effort.


<a href="https://riverscapes.xyz"><img class="float-left" src="https://riverscapes.xyz/assets/images/rc/RiverscapesConsortium_Logo_Black_BHS_200w.png"></a>
The [Riverscapes Consortium's](https://riverscapes.xyz) Technical Committee provides report cards for tools either deemed as "[riverscapes-compliant](https://riverscapes.xyz/Tools/#riverscapes-compliant)" <img  src="https://riverscapes.xyz/assets/images/rc/RiverscapesCompliant_24.png"> or "[pending riverscapes-compliance](https://riverscapes.xyz/Tools/#tools-pending-riverscapes-compliance)" <img  src="https://riverscapes.xyz/assets/images/rc/RiverscapesCompliantPending_28.png">.
