# 2026-04-11

There are 3 separate formats for the data:

* CSV download 
* GPGK download
* ESRI feature service

They all have slightly different set of columns and the way they name them is different. 
I noticed the GPKG columns seem to be different in a version download Jan vs April of 2026.
The feature service allows selection of a sub-area - so we used that as an API call in rs-reports-gen to fetch latest version of the data, and only what's needed.

For column names, descriptions, see the PDF data dictionary. The file name when you download it, says "June 2025" but the contents say August 2024.  

I assembled this somewhat manually in google sheet, export to CSV, use the rsxml script to convert to layer_definitions.json.
