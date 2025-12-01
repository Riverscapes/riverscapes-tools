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
        if filename == "project.rs.xml":
            project_files.append(os.path.join(dirpath, filename))


# Remove any projects that don't contain "Yankee Fork" in their path
# because the folder might contain other post-champ surveys, surch as Humboldt County etc
print(f"Found {len(project_files)} total projects.")
yankee_fork_projects = [pf for pf in project_files if "YankeeFork" in pf]
print(f"Found {len(yankee_fork_projects)} Yankee Fork projects to migrate.")

# Loop over each project and migrate it
commands = []
for project_file in yankee_fork_projects:

    tree = ET.parse(project_file)
    orig_xml = tree.getroot()

    site = orig_xml.find('MetaData/Meta[@name="Site"]').text
    visit = orig_xml.find('MetaData/Meta[@name="Visit"]').text
    watershed = orig_xml.find('MetaData/Meta[@name="Watershed"]').text
    year = orig_xml.find('MetaData/Meta[@name="Year"]').text
    migrated_on = orig_xml.find('MetaData/Meta[@name="MigratedOn"]')

    if migrated_on is None:
        print(f"Skipping project without MigratedOn meta: {project_file}")
        continue

    warehouse = orig_xml.find('Warehouse')
    if warehouse is not None:
        print('Skipping project with existing Warehouse node:', project_file)
        continue

    print(f"Processing project file: {project_file}")
    commands.append(f'rscli upload --org {owner_guid} "{project_file}"')

# Write commands to a shell script
script_path = os.path.join(parent_dir, 'upload_yankee_fork_projects.sh')
with open(script_path, 'w', encoding='utf-8') as f:
    f.write("#!/bin/bash\n\n")
    for cmd in commands:
        f.write(cmd + "\n")