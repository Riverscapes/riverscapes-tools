import subprocess
import os


def upload_projects(directory: str, org: str, tags: str):
    """iterate through the subdirectories in the given directory and upload them to the data exchange
    under ownership of the specified organization

    Args:
        directory (str): path to the directory containing the projects to be uploaded
        org (str): data exchange organization guid
    """

    subdirs = os.listdir(directory)
    for subdir in subdirs:
        subprocess.run(["rscli", "upload", os.path.join(directory, subdir), "--org", org, "--tags", tags])


in_dir = '/workspaces/data/beaver_activity'
in_org = '06439423-ee19-4040-9ebd-01c6e481a763'
in_tags = 'MT_Dam_Census'

upload_projects(in_dir, in_org, in_tags)
