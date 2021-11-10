---
title: Downloading BRAT Data
weight: 1
---

## Downloading Data
BRAT data can be downloaded frome the [Riverscapes Data Warehouse](https://data.riverscapes.xyz). Select the program containing the BRAT data you wish to download. Once inside the program, the project type can be filtered to only BRAT projects using the drop down at the top left of the screen.

![filter_project_type]({{site.baseurl }}/assets/images/filter_project_type.png)

 Navigate to your watershed of interest by browsing or searching for it's 8-digit hydrolgic unit code (HUC). To download the BRAT project, click on the cloud with the arrow and save the project to your machine.

![download_data]({{site.baseurl }}/assets/images/download_data.png)

## Viewing/Exploring Data
BRAT Riverscapes projects can be viewed using the [Riverscapes Analysis Visualilzation Explorer](https://rave.riverscapes.xyz) (RAVE). RAVE is available in three varieties:
- [ArcRAVE](https://rave.riverscapes.xyz/Download/install_arcrave.html) for ArcGIS users.
- [QRAVE](https://rave.riverscapes.xyz/Download/install_qrave.html) for QGIS users.
- WebRAVE(https://rave.riverscapes.xyz/Download/install_webrave.html), a webgis that can be used to explor riverscapes projects in a web browser. To open projects in WebRAVE, simply click on the globe icon next to the project in the data warehouse ![view_in_webrave]({{site.baseurl }}/assets/images/view_in_webrave.png)

When using **ArcRAVE** or **QRAVE**, import the project by clicking on the `Open Riverscapes Project` button in the plugin, navigating to the project folder, and selecting the project.rs.xml file. This will bring in the entire BRAT project, structured into a project tree, along with all of the default symbology to visualize outputs.

Once you have downloaded the data, you can begin [working with BRAT outputs](https://tools.riverscapes.xyz/brat/docs/Getting%20Started/WorkingWithOutputs.html)