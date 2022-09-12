```
---
title: Stream Gradient
---
```

Stream gradient describes an increase or decrease in the elevation of the stream channel over a longitudinal distance.

<img src="https://user-images.githubusercontent.com/73319684/182425741-dc11be3e-ffda-47f3-90b5-62802619d6d0.png" alt="image" style="zoom:50%;" />

*From Earth Communications, Chapter 4: Earth Surfaces, Section 4: High Gradient Streams*

## Why is this metric important?

* Helps predict debris flow transport and deposition (Fannin, R.J. and Rollerson, T.P., 1993),
* Helps to estimate distribution of aquatic organisms (Hicks, Brendan, J., and Hall, J.D., 2003), 
* Is a predictor of channel morphology



## Metric info:

**Machine Code:** STRMGRAD

**Analysis Window**

| Small | Medium | Large |
| ----- | ------ | ----- |
| 100 m | 250 m  | 1 km  |

**Lines of Evidence**

| LOE            | Measurement                                                  |
| -------------- | ------------------------------------------------------------ |
| DEM            | elevation start and end points in meters for each stream length* |
| Stream Network | length in meters of stream segment                           |

**Calculation**

`(𝑡𝑜𝑝 𝑜𝑓 𝑟𝑒𝑎𝑐ℎ 𝑒𝑙𝑒𝑣𝑎𝑡𝑖𝑜𝑛 − 𝑏𝑜𝑡𝑡𝑜𝑚 𝑜𝑓 𝑟𝑒𝑎𝑐ℎ 𝑒𝑙𝑒𝑣𝑎𝑡𝑖𝑜𝑛)/(𝑛𝑒𝑡𝑤𝑜𝑟𝑘 𝑙𝑒𝑛𝑔𝑡ℎ)`

**Process**

In small riverscapes, we look upstream and downstream 50 meters, to build a 100 meter analysis window. At the top and bottom of this window, we buffer the end point by 25 meters to find the lowest value cell in our input DEM. This ensures that we are actually looking at the channel elevation, instead of a potential floodplain or terrace elevation, which could happen if the stream network and DEM don't align perfectly. We then measure the length of the stream network within the analysis window. From there it is a simple rise over run calculation: `(𝑡𝑜𝑝 𝑜𝑓 𝑟𝑒𝑎𝑐ℎ 𝑒𝑙𝑒𝑣𝑎𝑡𝑖𝑜𝑛 − 𝑏𝑜𝑡𝑡𝑜𝑚 𝑜𝑓 𝑟𝑒𝑎𝑐ℎ 𝑒𝑙𝑒𝑣𝑎𝑡𝑖𝑜𝑛)/(𝑛𝑒𝑡𝑤𝑜𝑟𝑘 𝑙𝑒𝑛𝑔𝑡ℎ)`. 

The process is the same for medium and large riverscapes, except that the analysis windows are 250 meters and 1,000 meters, respectively, and the elevation search buffers are 50 meters and 100 meters, respectively. 

## Example:



## Citations:

*Fannin, R. J., & Rollerson, T. P. (1993). Debris flows: some physical characteristics and behaviour. Canadian Geotechnical Journal, 30(1), 71–81. https://doi.org/10.1139/t93-007*

*Hack, J. T. (1973). Stream-profile analysis and stream-gradient index. Journal of Research of the U.S. Geological Survey, 1(4), 421–429.*

*Hicks, B. J., & Hall, J. D. (2003). Rock Type and Channel Gradient Structure Salmonid Populations in the Oregon Coast Range. Transactions of the American Fisheries Society, 132(3), 468–482. https://doi.org/10.1577/1548-8659(2003)132<0468:RTACGS>2.0.CO;2*

*Nagel, D., Buffington, J., & Isaak, D. (2006). Comparison of methods for estimating stream channel gradient using GIS. USDA Forest Service, Rocky Mountain Research Station Boise Aquatic Sciences Lab.*



