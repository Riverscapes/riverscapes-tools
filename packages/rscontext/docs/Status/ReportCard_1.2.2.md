---
title: Riverscapes Report Card - Riverscapes Context 1.2.2
weight: 1
---

This report card communicates the Riverscape Context Tool's compliance with the Riverscape Consortium's published [tool standards](https://docs.riverscapes.net/standards/toolStandards)).

# Report Card Summary

| Tool | [Riverscapes Context Tool](https://tools.riverscapes.net/rscontext) |
| Version | [1.2.2](https://github.com/Riverscapes/riverscapes-tools/tree/master/packages/rscontext) |
| Date | 2021-11-22 |
| Assessment Team | Bailey, Wheaton & Gilbert |
| Current Assessment | ![production](https://raw.githubusercontent.com/Riverscapes/riverscapes-website/master/assets/images/tools/grade/TRL_6_32p.png) [Production Grade](https://docs.riverscapes.net/standards/discrimination) |
| Target Status | ![production](https://raw.githubusercontent.com/Riverscapes/riverscapes-website/master/assets/images/tools/grade/TRL_6_32p.png) Production Grade |
| Riverscapes Compliance | ![Compliant](https://riverscapes.net/assets/images/rc/RiverscapesCompliant_24.png) Compliant|
| Assessment Rationale | The tool can be used to aggregate data anywhere the nationally available datasets used to drive many riverscapes tools are available, and produces riverscapes projects that are compatible with [RAVE](https://rave.riverscapes.net). These projects are also being uploaded to the riverscapes warehouse. The tool needs documentation, but qualifies for Production Grade. |


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
| Scale | Catchment scale | <i class="fa fa-battery-full" aria-hidden="true"></i> | This tool is designed to aggregate data at the HUC catchment scale. Most commonly we run this at HUC 8 scale, but HUC 10 and HUC 12 runs have been run. HUC 6 runs are possible but get to be large projects and custom boundaries are possible. |
| Language(s) and Dependencies | Python | <i class="fa fa-battery-full" aria-hidden="true"></i> | Package dependencies are open source |
| Vetted in Peer-Reviewed Literature | Yes | <i class="fa fa-battery-full" aria-hidden="true"></i> | Not applicable. The datasets the tool gathers are vetted, nationally available datasets. |
| Source Code Documentation | Available at [Github repository](https://github.com/Riverscapes/riverscaps-tools/rscontext)<i class="fa fa-github" aria-hidden="true"></i> | <i class="fa fa-battery-full" aria-hidden="true"></i> | Source code is clearly organized and documented |
| Open Source | [open-source](https://github.com/Riverscapes/riverscaps-tools/rscontext) <i class="fa fa-github" aria-hidden="true"></i> with [GNU General Public License v 3.0](https://github.com/Riverscapes/riverscapes-tools/blob/master/LICENSE) | <i class="fa fa-battery-full" aria-hidden="true"></i> | |
| User Documentation | [Documentation](http://tools.riverscapes.net/channel/) is in progress | <i class="fa fa-battery-quarter" aria-hidden="true"></i> | Documentation is in progress but has not been published to the website |
| Easy User Interface | Tool is accessed via command prompt | <i class="fa fa-battery-quarter" aria-hidden="true"></i> | The tool is primarily run by North Arrow Research analysts. For a user to run this version of the tool themselves would require advanced understanding (programming capability) and significant documentation on the process. A "web-store" was piloted where users could request a HUC 8, 10 or 12 and get a run produced in cloud for them and uploaded to warehouse. That works and is available by request, but will be added to Warehouse 2.0. |
| Scalability | batch-processing | <i class="fa fa-battery-full" aria-hidden="true"></i> | Tool is typically batch processed at the HUC 8 level. If less layers are included, it can be run at larger scales. |
| Produces Riverscapes Projects <img  src="https://riverscapes.net/assets/images/data/RiverscapesProject_24.png"> | Yes | <i class="fa fa-battery-full" aria-hidden="true"></i> | Produces Riverscapes Projects that are fully-compatible with [RAVE](https://rave.riverscapes.net) |

## Tool Output Utility

| Criteria | Value | Evaluation | Comments |
|----------|-------|------------|----------|
| :------- | :---- | :--------- | :------- |
| [RAVE](https://rave.riverscapes.net)- Compliant Riverscapes Projects <img  src="https://riverscapes.net/assets/images/data/RiverscapesProject_24.png"> | Yes | <i class="fa fa-battery-full" aria-hidden="true"></i> | |
| [RAVE](https://rave.riverscapes.net) Business Logic Defined | Yes | <i class="fa fa-battery-full" aria-hidden="true"></i> |  |
| Riverscapes Projects hosted in public-facing [Riverscapes Data Exchange](https://riverscapes.net) <img src="https://riverscapes.net/assets/images/data/RiverscapesWarehouseCloud_24.png"> | Yes. Current Riverscape Context projects are being housed in the [Riverscapes Warehouse](https://data.riverscapes.net). | <i class="fa fa-battery-half" aria-hidden="true"></i> | There is data in the warehouse, but is provisional and wider access to the warehouse is forthcoming |
| Riverscapes Projects connected to [Web-Maps](https://riverscapes.net/software-help/help-web) <i class="fa fa-map-o" aria-hidden="true"></i> | Yes | <i class="fa fa-battery-full" aria-hidden="true"></i> | Current projects are viewable in [WebRAVE](https://rave.riverscapes.net/Download/install_webrave.html) through the Riverscapes Warehouse |
| Riverscapes Projects connected to Field [Apps](https://docs.riverscapes.net/category/products) <img src="http://riverscapes.net/assets/images/tools/PWA.png"> | No | <i class="fa fa-battery-quarter" aria-hidden="true"></i> | These can be used as basemaps or context layers with QField. |

## Developer Intent
...
The Riverscapes Context was one of our first production grade tools. It has worked extremely well and has now been successfully run on 2/3 of the 2800 HUC 8s in the United States and those projects are freely availalbe. In due course, a sponsor will pay to have it run for the rest of the US. 

Future enhancements under consideration include:
- Updating RS_Conext to [use geopackages instead of shapefiles](https://github.com/Riverscapes/riverscapes-tools/issues/135) for some of the vector feature classes it "wraps up". The tool has tended not to change from the parent format of vectors (typically awful shapefiles with all their sidecar files), but it does change rasters into GeoTIFFs. There are many advantages to geopackages over shapefiles for vector data. If this is done, it will likely be with one geopackage for each data type source (e.g. one for NHD and one for Land Ownership). 
- Continue to add new layers that are useful to users for context or as inputs to other production grade models. A running list is [here]( https://github.com/Riverscapes/riverscapes-tools/issues?q=is%3Aopen+is%3Aissue+label%3Aenhancement+label%3Apkg%3ARS-Context)
- Expand functionality so it can work for any user provided custom polygon extent (provided that underlyng context datasets exist for that area)
- Expand functionality to other countries (initially those that have consistent, good cartographic standards) and eventually to global datasets



<a href="https://riverscapes.net"><img class="float-left" src="https://riverscapes.net/assets/images/rc/RiverscapesConsortium_Logo_Black_BHS_200w.png"></a>
The [Riverscapes Consortium's](https://riverscapes.net) Technical Committee provides report cards for tools either deemed as "[riverscapes-compliant](https://docs.riverscapes.net/standards
)" <img  src="https://riverscapes.net/assets/images/rc/RiverscapesCompliant_24.png"> or "[pending riverscapes-compliance](https://riverscapes.net/Tools/#tools-pending-riverscapes-compliance)" <img  src="https://riverscapes.net/assets/images/rc/RiverscapesCompliantPending_28.png">.
