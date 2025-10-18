gdalbuildvrt -separate \
    /Users/philipbailey/GISData/riverscapes/pytorch_vbet/stacked.vrt \
    /Users/philipbailey/GISData/riverscapes/pytorch_vbet/taudem/outputs/hand.tif \
    /Users/philipbailey/GISData/riverscapes/pytorch_vbet/taudem/outputs/gdal_slope.tif \
    /Users/philipbailey/GISData/riverscapes/pytorch_vbet/output_distance.tif \
    /Users/philipbailey/GISData/riverscapes/pytorch_vbet/drainage_area.tif


gdal_translate /Users/philipbailey/GISData/riverscapes/pytorch_vbet/stacked.vrt /Users/philipbailey/GISData/riverscapes/pytorch_vbet/stacked.tif