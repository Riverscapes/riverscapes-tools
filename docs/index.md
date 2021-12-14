---
title: Home
---
# RC Production-Grade, Network-Scale Tools
Welcome to the documentation for the [Riverscape Consortium's](https://riverscapes.xyz) own [_production-grade_ ![pgt](https://riverscapes.xyz/assets/images/tools/grade/TRL_6_32p.png)](https://riverscapes.xyz/Tools/discrimination.html#tool-grade) family of network-scale riverscape models. These are  [riverscapes-compliant](https://riverscapes.xyz/Tools/#riverscapes-compliant) ![compliant]({{ site.baseurl }}/assets/images/rc/RiverscapesCompliant_24.png) tools, which have been refactored to be capable of running in the cloud over regional, state-wide and/or nation-wide extents, while still resolving predictions of what is happening in individual reaches (i.e. 100 m to 500 m length scales) of riverscape.

Unlike some of our  [_operational-grade_ ![pgt](https://riverscapes.xyz/assets/images/tools/grade/TRL_4_32p.png)](https://riverscapes.xyz/Tools/discrimination.html#tool-grade) and  [_professional-grade_ ![pgt](https://riverscapes.xyz/assets/images/tools/grade/TRL_5_32p.png)](https://riverscapes.xyz/Tools/discrimination.html#tool-grade) GIS tools, which "users" run the models themselves in desktop GIS, these tools are run centrally in the cloud and their outputs are consumed by users through [RAVE](http://rave.riverscapes.xyz) in the web browser or through desktop GIS.  Users can access tool outputs, which our packaged as [riverscapes projects ![project](https://riverscapes.xyz/assets/images/data/RiverscapesProject_24.png)](https://riverscapes.xyz/Tools/Technical_Reference/Documentation_Standards/Riverscapes_Projects/) in our  [data warehouse ![warehouse](https://riverscapes.xyz/assets/images/data/RiverscapesWarehouseCloud_24.png)](https://data.riverscapes.xyz/).  


## Production-Grade Network-Scale RC Tool Documentation:

<div class="row small-up-2 medium-up-3">

  <div class="column">
    <div class="card">
      <a href="https://tools.riverscapes.xyz/rscontext" target="blank"><img align="center" src="{{ site.baseurl }}/assets/images/tools/RSC_Tile.png"></a>
      <div class="card-section">
        <h4>Riverscapes Context</h4>
        <p>The <a href="https://tools.riverscapes.xyz/rscontext/"><b>Riverscapes Context</b></a>  tool automatically aggregates a variety of nationally available spatial datasets into a riverscapes project for a given watershed (e.g. HUC8). These data can then be used for contextual purposes, mapping, or as input data in other tools. <img src="{{ site.baseurl }}/assets/images/rc/RiverscapesCompliant_24.png"></p>
      </div>
    </div>
  </div>

  <div class="column">
    <div class="card">
      <a href="https://tools.riverscapes.xyz/channel" target="blank"><img align="center" src="{{ site.baseurl }}/assets/images/tools/ChannelArea_Tile.png"></a>
      <div class="card-section">
        <h4>Channel Area</h4>
        <p>The <a href="https://tools.riverscapes.xyz/channel" targetj="blank"><b>Channel Area</b></a>  tool generates polygons representing the active channel network within a watershed. <img src="{{ site.baseurl }}/assets/images/rc/RiverscapesCompliant_24.png"></p>
      </div>
    </div>
  </div>

  <div class="column">
    <div class="card">
      <a href="https://tools.riverscapes.xyz/taudem" target="blank"><img align="center" src="{{ site.baseurl }}/assets/images/tools/TauDEM_Tile.png"></a>
      <div class="card-section">
        <h4>TauDEM</h4>
        <p>The <a href="https://tools.riverscapes.xyz/taudem"><b>TauDEM</b></a> is a suite of tools for topographic analysis using DEMs. <img src="{{ site.baseurl }}/assets/images/rc/RiverscapesCompliant_24.png"></p>
      </div>
    </div>
  </div>

</div>

<div class="row small-up-2 medium-up-3">

  <div class="column">
    <div class="card">
      <a href="https://tools.riverscapes.xyz/vbet"><img align="center" src="{{ site.baseurl }}/assets/images/tools/VBET_Tile.png"></a>
      <div class="card-section">
        <h4>VBET</h4>
        <p>The <a href="https://tools.riverscapes.xyz/vbet"><b>Valley Bottom Extraction Tool</b></a> uses a DEM and a channel area network to estimate the valley bottom extents (area that could plausibly flood in contemporary natural flow regime) thereby defining the riverscape network. <img src="{{ site.baseurl }}/assets/images/rc/RiverscapesCompliant_24.png"></p>
      </div>
    </div>
  </div>

  <div class="column">
    <div class="card">
      <a href="https://tools.riverscapes.xyz/brat" target="blank"><img align="center" src="{{ site.baseurl }}/assets/images/tools/BRAT_Tile.png"></a>
      <div class="card-section">
        <h4>BRAT</h4>
        <p>The <a href="https://tools.riverscapes.xyz/brat"><b>Beaver Restoration Assessment Tool</b></a> combines a model estimating the capacity of a riverscape to support dam building activity with analysis of potential anthropogenic conflicts to create a tool that can be used to inform where <a href ="http://lowtechpbr.restoration.usu.edu/">LTPBR</a>restoration using beaver can be targeted. <img src="{{ site.baseurl }}/assets/images/rc/RiverscapesCompliant_24.png"></p>
      </div>
    </div>
  </div>
  <!--
  <div class="column">
    <div class="card">
      <a href="https://tools.riverscapes.xyz/brat"><img align="center" src="{{ site.baseurl }}/assets/images/tools/BRAT_Tile.png"></a>
      <div class="card-section">
        <h4>BRAT</h4>
        <p>The <a href="https://tools.riverscapes.xyz/brat"><b>Beaver Restoration Assessment Tool</b></a> combines a model estimating the capacity of a riverscape to support dam building activity with analysis of potential anthropogenic conflicts to create a tool that can be used to inform where <a href ="http://lowtechpbr.restoration.usu.edu/">LTPBR</a>restoration using beaver can be targeted. <img src="{{ site.baseurl }}/assets/images/rc/RiverscapesCompliant_24.png"></p>
      </div>
    </div>
  </div>
  --->
</div>




### Tools Pending RS Compliance & Production Grade Status
These tools are undergoing refactoring to Production-Grade status.

* [**RVD**](https://tools.riverscapes.xyz/rvd): **Riparian Vegetation Departure** uses nationally available vegetation classification datasets to estimate change in riparian vegetation cover from historic conditions within riverscapes. ![pending]({{ site.baseurl }}/assets/images/rc/RiverscapesCompliantPending_28.png)
<!--- * [**Confinement**](https://tools.riverscapes.xyz/cofinement): The **Confinement Tool** calculates the valley confinement for each segment of a drainage network. ![pending]({{ site.baseurl }}/assets/images/rc/RiverscapesCompliantPending_28.png) -->

## Support
The RC is committed to supporting users and consumers of these tool outputs. We encourage questions regarding tools and their use through our online support forum [here](https://github.com/Riverscapes/riverscapes-tools/discussions):

<div align="center"><a class="button" href="https://github.com/Riverscapes/riverscapes-tools/discussions"><i class="fa fa-github"></i> Tool Discussion</a></div>

Note that a [free GitHub <i class="fa fa-github"></i> account](https://github.com/signup?ref_cta=Sign+up&ref_loc=header+logged+out&ref_page=%2F&source=header-home) is required to post to the [discussion board](https://github.com/Riverscapes/riverscapes-tools/discussions) but you can browse without an account.

_As of December 2021, documentation has only just begun and will be actively updated through winter 2022._
