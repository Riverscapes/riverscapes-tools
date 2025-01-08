import os
from rscommons import RSProject


def census_info(project_dir: str) -> int:
    project = RSProject(None, os.path.join(project_dir, 'project.rs.xml'))
    x = project.XMLBuilder.find('Realizations')
    realizations = x.findall('Realization')
    rs = [r for r in realizations if r.attrib['id'] != 'inputs']
    r_out = {r: [r.find('Description').text, r.find('Name').text] for r in rs}

    return r_out
