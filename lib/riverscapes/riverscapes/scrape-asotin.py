import csv
from riverscapes import RiverscapesAPI, RiverscapesSearchParams

csv_file = "asotin_projects.csv"

search_params = RiverscapesSearchParams({
    'tags': ["AsotinIMW"],
})

projects = []
with RiverscapesAPI(stage='production') as api:
    for project, _stats, search_total, _prg in api.search(search_params, progress_bar=True):

        project_type = project.project_type
        visit_id = project.project_meta.get('Visit', None)
        site_name = project.project_meta.get('Site', None)
        watershed = project.project_meta.get('Watershed', None)
        year = project.project_meta.get('Year', None)

        if visit_id is None:
            continue

        projects.append({
            'id': project.id,
            'type': project_type,
            'visit_id': visit_id,
            'site_name': site_name,
            'watershed': watershed,
            'year': year
        })

print(f'Found {len(projects)} projects with tag AsotinIMW')

# Write objects to a CSV file
with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)

    # Write the header row
    writer.writerow(["guid", "type", "visit_id", "site_name", "watershed", "year"])

    # Write each object's attributes
    for item in projects:
        writer.writerow([item['id'], item['type'], item['visit_id'], item['site_name'], item['watershed'], item['year']])
