from cybercastor.lib.file_download import download_files

dl_dir = '/path/to/some/dir'  # where you download to locally
hucs = []  # fill in hucs - this can be HUC8s
files = []  # fill in specific files to download within the project

for huc in hucs:
    download_files('production', 'vbet', huc, ['vbet_intermediates.gpkg'], dl_dir)
