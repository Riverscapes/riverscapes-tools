"""
One time script to upload post-champ Yankee Fork topo projects. These projects
were migrated to V2 XML using a script in RiverscapesXML repo.

This script simply loops over all the projects and writes a Shell script to write rscli 
commands."""

import os
import xml.etree.ElementTree as ET

parent_dir= '/Users/philipbailey/GISData/champ/MonitoringDataUnzipped'
owner_guid = '9a619d52-6b3c-4e26-8854-eb7ff9b77c2a' # Watershed Solutions

project_files = []
for dirpath, __dirnames, filenames in os.walk(parent_dir):
    for filename in filenames:
        if filename == "project.rs.xml.bak":
            project_files.append(os.path.join(dirpath, filename))


for project_file in project_files:
    new_file = os.path.join(os.path.dirname(project_file), 'backup_project.xml')
    print(f'Renaming {project_file} to {new_file}')
    os.rename(project_file, new_file)
    