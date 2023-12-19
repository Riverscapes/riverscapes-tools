import requests
import json


# Project types and their associated json urls for RV web symbology from which to get colors
PROJECT_TYPES = {
    'RSCONTEXT': ['https://raw.githubusercontent.com/Riverscapes/RiverscapesXML/master/Symbology/web/Shared/flow_lines.json',
                  'https://raw.githubusercontent.com/Riverscapes/RiverscapesXML/master/Symbology/web/Shared/roads.json'],
    'BRAT': ['https://raw.githubusercontent.com/Riverscapes/RiverscapesXML/master/Symbology/web/BRAT/opportunity.json'],
}


def convert_colors(hsl_string):
    """Converts HSL string to RGB (0-1) values"""
    if 'hsla' in hsl_string:
        hsl_string = hsl_string.replace('hsla', 'hsl')
    hsl = hsl_string.replace('hsl(', '').replace(')', '').split(',')
    h = int(hsl[0])
    s = int(hsl[1].replace('%', '')) / 100
    l = int(hsl[2].replace('%', '')) / 100

    c = (1 - abs(2*l - 1)) * s
    x = c * (1 - abs((h/60) % 2 - 1))
    m = l - c/2

    if h < 60:
        r, g, b = c, x, 0
    elif 60 <= h < 120:
        r, g, b = x, c, 0
    elif 120 <= h < 180:
        r, g, b = 0, c, x
    elif 180 <= h < 240:
        r, g, b = 0, x, c
    elif 240 <= h < 300:
        r, g, b = x, 0, c
    elif 300 <= h < 360:
        r, g, b = c, 0, x

    r, g, b = (r+m), (g+m), (b+m)

    return (r, g, b)


def color_dict(json_url):
    """Returns a dictionary of colors from a json url for RV web symbology"""

    colors_tmp = {}

    r = requests.get(json_url, timeout=5)
    data = json.loads(r.text)

    legend = data['legend']
    for key in legend:
        if '(dashed)' in key[1]:
            key[1] = key[1].replace('(dashed)', '').strip()
        colors_tmp[key[1]] = key[0]
        
    colors = {lab: convert_colors(hsl) for lab, hsl in colors_tmp.items()}

    return colors


def get_colors(project_type):
    """Returns a dictionary of colors for a given project type"""

    colors = {}

    if project_type in PROJECT_TYPES:
        for url in PROJECT_TYPES[project_type]:
            colors.update(color_dict(url))

    return colors
