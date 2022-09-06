# Valley Gradient

Valley gradient describes an increase or decrease in the elevation of the valley bottom over a longitudinal distance.

![Untitled](https://user-images.githubusercontent.com/73319684/188728735-5dd5c7dd-cb1f-4ccf-8c81-07633bcdcc1e.png)

*Created with BioRender.com*

### Why is this metric important?

* Is a predictor of channel morphology

### Metric info:

**Machine Code:** VALGRAD

**Analysis Window**

| Small | Medium | Large |
| ----- | ------ | ----- |
| 100 m | 250 m  | 1 km  |

**Lines of Evidence**

| LOE                   | Measurement                                                  |
| --------------------- | ------------------------------------------------------------ |
| DEM                   | elevation start and end points in meters for each valley bottom segment length |
| Valley bottom segment | length in meters of each valley bottom segment               |

**Calculation**

`(𝑡𝑜𝑝 𝑜𝑓 𝑠𝑒𝑔𝑚𝑒𝑛𝑡 𝑒𝑙𝑒𝑣. −𝑏𝑜𝑡𝑡𝑜𝑚 𝑜𝑓 𝑠𝑒𝑔𝑚𝑒𝑛𝑡 𝑒𝑙𝑒𝑣.)/(𝑣𝑎𝑙𝑙𝑒𝑦 𝑏𝑜𝑡𝑡𝑜𝑚 𝑐𝑒𝑛𝑡𝑒𝑟𝑙𝑖𝑛𝑒)`

**Process**

In small riverscapes, we look upstream and downstream 50 meters, to build a 100 meter analysis window. At the top and bottom of this window, we buffer the end point by 25 meters to find the lowest value cell in our input DEM. This ensures that we are actually looking at the channel elevation, instead of a potential floodplain or terrace elevation, which could happen if the stream network and DEM don't align perfectly. We then measure the length of the stream network within the analysis window. From there it is a simple rise over run calculation: `(𝑡𝑜𝑝 𝑜𝑓 𝑠𝑒𝑔𝑚𝑒𝑛𝑡 𝑒𝑙𝑒𝑣. −𝑏𝑜𝑡𝑡𝑜𝑚 𝑜𝑓 𝑠𝑒𝑔𝑚𝑒𝑛𝑡 𝑒𝑙𝑒𝑣.)/(𝑣𝑎𝑙𝑙𝑒𝑦 𝑏𝑜𝑡𝑡𝑜𝑚 𝑐𝑒𝑛𝑡𝑒𝑟𝑙𝑖𝑛𝑒)`. 

The process is the same for medium and large riverscapes, except that the analysis windows are 250 meters and 1,000 meters, respectively, and the elevation search buffers are 50 meters and 100 meters, respectively. 

### Example:

### Citations:





