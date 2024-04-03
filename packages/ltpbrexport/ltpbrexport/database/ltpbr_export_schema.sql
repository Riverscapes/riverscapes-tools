CREATE TABLE States
(
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    iso_code TEXT,
    url TEXT UNIQUE NOT NULL,
    metadata TEXT,
    created_at datetime NOT NULL,
    updated_at datetime NOT NULL
);

CREATE TABLE organizations
(
    id serial PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    contact TEXT,
    website TEXT,
    url TEXT NOT NULL,
    metadata TEXT,
    created_at datetime NOT NULL,
    updated_at datetime NOT NULL
);

CREATE TABLE project_organizations
(
    project_id INTEGER NOT NULL REFERENCES projects(fid),
    organization_id INTEGER NOT NULL REFERENCES organizations(id),

    PRIMARY KEY (project_id, organization_id)
);
CREATE INDEX project_organizations_organization_id ON project_organizations(organization_id);