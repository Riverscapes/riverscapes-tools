---
title: Riverscapes Report Card - Riverscapes Context 1.2.2
weight: 1
---

This report card communicates the Riverscape Context Tool's compliance with the Riverscape Consortium's published [tool standards](https://riverscapes.xyz/Tools).

# Report Card Summary

| Tool | [Riverscapes Context Tool](https://tools.riverscapes.xyz/rscontext) |
| Version | [1.2.2](https://github.com/Riverscapes/riverscapes-tools/tree/master/packages/rscontext) |
| Date | 2021-11-22 |
| Assessment Team | Bailey, Wheaton & Gilbert |
| Current Assessment | ![production](https://raw.githubusercontent.com/Riverscapes/riverscapes-website/master/assets/images/tools/grade/TRL_6_32p.png) [Production Grade](https://riverscapes.xyz/Tools/#tool-grade) |
| Target Status | ![production](https://raw.githubusercontent.com/Riverscapes/riverscapes-website/master/assets/images/tools/grade/TRL_6_32p.png) Production Grade |
| Riverscapes Compliance | ![Compliant](https://riverscapes.xyz/assets/images/rc/RiverscapesCompliant_24.png) Compliant|
| Assessment Rationale | The tool can be used to aggregate data anywhere the nationally available datasets used to drive many riverscapes tools are available, and produces riverscapes projects that are compatible with [RAVE](https://rave.riverscapes.xyz). These projects are also being uploaded to the riverscapes warehouse. The tool needs documentation, but qualifies for Production Grade. |


# Report Card Details

This tool's [discrimination](https://riverscapes.xyz/Tools/#model-discrimination) evaluation by the [Riverscapes Consortium](https://riverscapes.xyz) is:

**Evaluation Key:**
None or Not Applicable: <i class="fa fa-battery-empty" aria-hidden="true"></i> •
Minimal or In Progress: <i class="fa fa-battery-quarter" aria-hidden="true"></i> •
Functional: <i class="fa fa-battery-half" aria-hidden="true"></i> •
Fully Developed: <i class="fa fa-battery-full" aria-hidden="true"></i>  

| Criteria | Value | Evaluation | Comments and/or Recommendations |
|----------|-------|------------|---------------------------------|
| :------- | :---- | :--------  | :------------------------------ |
| Tool Interface(s) | <i class="fa fa-terminal" aria-hidden="true"></i> : CLI = [Command Line Interface](https://en.wikipedia.org/wiki/Command-line_interface) | <i class="fa fa-battery-full" aria-hidden="true"></i> |  |
| Scale | Catchment scale | <i class="fa fa-battery-full" aria-hidden="true"></i> | This tool is designed to aggregate data at the HUC 8 catchment scale |
| Language(s) and Dependencies | Python | <i class="fa fa-battery-full" aria-hidden="true"></i> | Package dependencies are open source |
| Vetted in Peer-Reviewed Literature | Yes | <i class="fa fa-battery-full" aria-hidden="true"></i> | The datasets the tool gathers are vetted, nationally available datasets |
| Source Code Documentation | Available at [Github repository](https://github.com/Riverscapes/riverscaps-tools/rscontext)<i class="fa fa-github" aria-hidden="true"></i> | <i class="fa fa-battery-full" aria-hidden="true"></i> | Source code is clearly organized and documented |
| Open Source | [open-source](https://github.com/Riverscapes/riverscaps-tools/rscontext) <i class="fa fa-github" aria-hidden="true"></i> with [GNU General Public License v 3.0](https://github.com/Riverscapes/riverscapes-tools/blob/master/LICENSE) | <i class="fa fa-battery-full" aria-hidden="true"></i> | |
| User Documentation | [Documentation](http://tools.riverscapes.xyz/channel/) is in progress | <i class="fa fa-battery-quarter" aria-hidden="true"></i> | Documentation is in progress but has not been published to the website |
| Easy User Interface | Tool is accessed via command prompt | <i class="fa fa-battery-quarter" aria-hidden="true"></i> | The tool is primarily run by North Arrow Research analysts. For a user to run this version of the tool themselves would require advanced understanding (programming capability) and significant documentation on the process. |
| Scalability | batch-processing | <i class="fa fa-battery-full" aria-hidden="true"></i> | Tool is typically batch processed at the HUC 8 level |
| Produces Riverscapes Projects <img  src="https://riverscapes.xyz/assets/images/data/RiverscapesProject_24.png"> | Yes | <i class="fa fa-battery-full" aria-hidden="true"></i> | Produces Riverscapes Projects that are fully-compatible with [RAVE](https://rave.riverscapes.xyz) |

## Tool Output Utility

| Criteria | Value | Evaluation | Comments |
|----------|-------|------------|----------|
| :------- | :---- | :--------- | :------- |
| [RAVE](https://rave.riverscapes.xyz)- Compliant Riverscapes Projects <img  src="https://riverscapes.xyz/assets/images/data/RiverscapesProject_24.png"> | Yes | <i class="fa fa-battery-full" aria-hidden="true"></i> | |
| [RAVE](https://rave.riverscapes.xyz) Business Logic Defined | Yes | <i class="fa fa-battery-full" aria-hidden="true"></i> |  |
| Riverscapes Projects hosted in public-facing [Riverscapes Warehouse(s)](https://riverscapes.xyz/Data_Warehouses/#warehouse-explorer-concept) <img src="https://riverscapes.xyz/assets/images/data/RiverscapesWarehouseCloud_24.png"> | Yes. Current Riverscape Context projects are being housed in the [Riverscapes Warehouse](https://data.riverscapes.xyz). | <i class="fa fa-battery-half" aria-hidden="true"></i> | There is data in the warehouse, but is provisional and wider access to the warehouse is forthcoming |
| Riverscapes Projects connected to [Web-Maps](https://riverscapes.xyz/Data_Warehouses#web-maps) <i class="fa fa-map-o" aria-hidden="true"></i> | Yes | <i class="fa fa-battery-full" aria-hidden="true"></i> | Current projects are viewable in [WebRAVE](https://rave.riverscapes.xyz/Download/install_webrave.html) through the Riverscapes Warehouse |
| Riverscapes Projects connected to Field [Apps](https://riverscapes.xyz//Data_Warehouses#apps---pwas) <img src="http://riverscapes.xyz/assets/images/tools/PWA.png"> | No | <i class="fa fa-battery-empty" aria-hidden="true"></i> |  |

## Developer Intent
...

<a href="https://riverscapes.xyz"><img class="float-left" src="https://riverscapes.xyz/assets/images/rc/RiverscapesConsortium_Logo_Black_BHS_200w.png"></a>
The [Riverscapes Consortium's](https://riverscapes.xyz) Technical Committee provides report cards for tools either deemed as "[riverscapes-compliant](https://riverscapes.xyz/Tools/#riverscapes-compliant)" <img  src="https://riverscapes.xyz/assets/images/rc/RiverscapesCompliant_24.png"> or "[pending riverscapes-compliance](https://riverscapes.xyz/Tools/#tools-pending-riverscapes-compliance)" <img  src="https://riverscapes.xyz/assets/images/rc/RiverscapesCompliantPending_28.png">.