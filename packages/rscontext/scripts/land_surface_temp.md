## Introduction



## Preprocessing
1. Mosaic the 8Day1km bands from all tiles for each scene (day) as "AYYYYJJJ.tif" (geotiff) into one folder
2. NHD: Reproject WBDHU8 and NHDPlusCatchments in custom modis projection. Append feature class name with "_reproject"
3. Add HUC8 field to NHDPlusCatchments_reproject and populate with huc8 id. (use centroids when making spatial selection, as the boundaries are not exactly the same between layers)

## Running the Script





## Performance comparisons

revision 3
AVG_TIMER::LoopTime:: Count: 184, Total Time: 31.309836s, Average: 170.162153ms  

revision 2
AVG_TIMER::LoopTime:: Count: 252, Total Time: 40.832026s, Average: 162.031851ms    