---
title: Riverscapes Report Card - Channel Area 1.1.1
weight: 1
---

This report card communicates the Channel Area Tool's compliance with the Riverscape Consortium's published [tool standards](https://docs.riverscapes.net/standards/toolStandards)).

# Report Card Summary

| Tool | [Channel Area Tool](https://tools.riverscapes.net/channel) |
| Version | [1.1.1](https://github.com/Riverscapes/riverscapes-tools/tree/master/packages/channel) |
| Date | 2021-11-22 |
| Assessment Team | Bailey, Wheaton & Gilbert |
| Current Assessment | ![production](https://raw.githubusercontent.com/Riverscapes/riverscapes-website/master/assets/images/tools/grade/TRL_5_32p.png) [Production Grade](https://docs.riverscapes.net/standards/discrimination) |
| Target Status | ![production](https://raw.githubusercontent.com/Riverscapes/riverscapes-website/master/assets/images/tools/grade/TRL_6_32p.png) Production Grade |
| Riverscapes Compliance | ![Compliant](https://riverscapes.net/assets/images/rc/RiverscapesCompliant_24.png) Compliant|
| Assessment Rationale | The tool has been run for a wide geographic area, produces riverscapes projects that are compatible with [RAVE](https://rave.riverscapes.net), and outputs are available in the riverscapes warehouse. Documentation is still needed, but the tool qualifies for Production Grade. |


# Report Card Details

This tool's [discrimination](https://docs.riverscapes.net/standards/toolStandards) evaluation by the [Riverscapes Consortium](https://riverscapes.net) is:

**Evaluation Key:**
None or Not Applicable: <i class="fa fa-battery-empty" aria-hidden="true"></i> •
Minimal or In Progress: <i class="fa fa-battery-quarter" aria-hidden="true"></i> •
Functional: <i class="fa fa-battery-half" aria-hidden="true"></i> •
Fully Developed: <i class="fa fa-battery-full" aria-hidden="true"></i>  

| Criteria | Value | Evaluation | Comments and/or Recommendations |
|----------|-------|------------|---------------------------------|
| :------- | :---- | :--------  | :------------------------------ |
| Tool Interface(s) | <i class="fa fa-terminal" aria-hidden="true"></i> : CLI = [Command Line Interface](https://en.wikipedia.org/wiki/Command-line_interface) | <i class="fa fa-battery-full" aria-hidden="true"></i> |  |
| Scale | Network (reach scale resolution, watershed extent) | <i class="fa fa-battery-full" aria-hidden="true"></i> | This tool has been applied across watersheds in various regions; results are reach-scale performed across whole watersheds |
| Language(s) and Dependencies | Python | <i class="fa fa-battery-full" aria-hidden="true"></i> | Package dependencies are open source |
| Vetted in Peer-Reviewed Literature | The equation used to estimate channel width in areas without NHD polygons is from  [Beechie and Imaki (2013)](https://agupubs.onlinelibrary.wiley.com/doi/full/10.1002/2013WR013629) | <i class="fa fa-battery-half" aria-hidden="true"></i> | The width equation used in the tool has been reliable in a variety of landscapes, but was developed for the Columbia River Basin and may give poor results in different landscapes |
| Source Code Documentation | Available at [Github repository](https://github.com/Riverscapes/riverscaps-tools/channel) <i class="fa fa-github" aria-hidden="true"></i> | <i class="fa fa-battery-full" aria-hidden="true"></i> | Source code is clearly organized and documented |
| Open Source | [open-source](https://github.com/Riverscapes/riverscaps-tools/channel) <i class="fa fa-github" aria-hidden="true"></i> with [GNU General Public License v 3.0](https://github.com/Riverscapes/riverscapes-tools/blob/master/LICENSE) | <i class="fa fa-battery-full" aria-hidden="true"></i> | |
| User Documentation | [Documentation](http://tools.riverscapes.net/channel/) is in progress | <i class="fa fa-battery-quarter" aria-hidden="true"></i> | Documentation is in progress but has not been published to the website |
| Easy User Interface | Tool is accessed via command prompt | <i class="fa fa-battery-quarter" aria-hidden="true"></i> | Most projects right now are performed by North Arrow Research analysts. For a user to run this version of the tool themselves would require advanced understanding (programming capability) and significant documentation on the process. |
| Scalability | Batch-processing | <i class="fa fa-battery-full" aria-hidden="true"></i> | The tool can be batch processed at the HUC 8 level |
| Produces Riverscapes Projects <img  src="https://riverscapes.net/assets/images/data/RiverscapesProject_24.png"> | Yes | <i class="fa fa-battery-full" aria-hidden="true"></i> | Produces Riverscapes Projects that are fully-compatible with [RAVE](https://rave.riverscapes.net) |

## Tool Output Utility

| Criteria | Value | Evaluation | Comments |
|----------|-------|------------|----------|
| :------- | :---- | :--------- | :------- |
| [RAVE](https://rave.riverscapes.net)- Compliant Riverscapes Projects <img  src="https://riverscapes.net/assets/images/data/RiverscapesProject_24.png"> | Yes | <i class="fa fa-battery-full" aria-hidden="true"></i> | |
| [RAVE](https://rave.riverscapes.net) Business Logic Defined | Yes | <i class="fa fa-battery-full" aria-hidden="true"></i> |  |
| Riverscapes Projects hosted in public-facing [Riverscapes Data Exchange](https://riverscapes.net) <img src="https://riverscapes.net/assets/images/data/RiverscapesWarehouseCloud_24.png"> | Yes. Current Channel Area projects are being housed in the [Riverscapes Warehouse](https://data.riverscapes.net). | <i class="fa fa-battery-half" aria-hidden="true"></i> | There is data in the warehouse, but is provisional and wider access to the warehouse is forthcoming |
| Riverscapes Projects connected to [Web-Maps](https://riverscapes.net/software-help/help-web) <i class="fa fa-map-o" aria-hidden="true"></i> | Yes | <i class="fa fa-battery-full" aria-hidden="true"></i> | Current projects are viewable in [WebRAVE](https://rave.riverscapes.net/Download/install_webrave.html) through the Riverscapes Warehouse |
| Riverscapes Projects connected to Field [Apps](https://docs.riverscapes.net/category/products) <img src="http://riverscapes.net/assets/images/tools/PWA.png"> | No | <i class="fa fa-battery-empty" aria-hidden="true"></i> |  |

## Developer Intent
The current **Production Grade** <img  src="https://riverscapes.net/assets/images/tools/grade/TRL_6_32p.png"> version of Channel Area improves on previous versions by:
- Having an inviting [web-map interface](https://viewer.riverscapes.net/software-help/help-web) so non GIS-users can discover tool runs and explore and interrogate them.
- Making it easy for GIS users to download channel area [projects](https://docs.riverscapes.net) for use in [RAVE](https://rave.riverscapes.net) <img  src="https://riverscapes.net/assets/images/data/RiverscapesProject_24.png">
- Allowing discovery of past Channel Area runs in Warehouse

The development team envision a **Professional Grade** <img src="https://riverscapes.net/assets/images/tools/grade/TRL_5_32p.png"> version of Channel Area, which would additionally:
- Encourage more user-interaction with Channel Area outputs and crowd-sourcing of information to improve the accuracy of outputs
- Allow users to upload their own Channel Area projects

<a href="https://riverscapes.net"><img class="float-left" src="https://riverscapes.net/assets/images/rc/RiverscapesConsortium_Logo_Black_BHS_200w.png"></a>
The [Riverscapes Consortium's](https://riverscapes.net) Technical Committee provides report cards for tools either deemed as "[riverscapes-compliant](https://docs.riverscapes.net/standards
)" <img  src="https://riverscapes.net/assets/images/rc/RiverscapesCompliant_24.png"> or "[pending riverscapes-compliance](https://riverscapes.net/Tools/#tools-pending-riverscapes-compliance)" <img  src="https://riverscapes.net/assets/images/rc/RiverscapesCompliantPending_28.png">.
