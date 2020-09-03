---
title: Database
weight: 2
---

BRAT uses a lightweight [SQLite](https://www.sqlite.org/index.html) database to store the information needed to run the model. This page describes the design of this database (referred to as a "[schema](https://en.wikipedia.org/wiki/Database_schema)") and how to work with it to refine the results. This is particularly important for the third and final [BRAT Run]({{ site.baseurl}}/Technical_Reference/architecture.html#3-brat-run) step that calculates dam capacity etc. All the parameters over which the user has control are stored in the database where they can be changed easily.

[SQLite](https://www.sqlite.org/index.html) is by default a command line tool, but there are several excellent products for viewing and working with SQLite databases. We recommend [SQLite Studio](https://sqlitestudio.pl) which is both free and can also simply be unzipped on your computer without a conventional installation process (that might require administrator privileges). You can also use Microsoft Access as the user interface with which to interact with SQLite databases, but [setting up this approach](https://xsviewer.northarrowresearch.com/Technical_Reference/working_with_sqlite_databases.html#microsoft-access) is a little involved.

Proficient database users can jump in and use the [BRAT database schema definition](https://github.com/Riverscapes/sqlBRAT/blob/master/database/brat_schema.sql), together with the default [lookup data](https://github.com/Riverscapes/sqlBRAT/tree/master/database/data) to create a new, empty copy of the BRAT database.

## Schema Overview

Each BRAT project contains an entire, self-contained copy of this database shown below. With care, you can edit all the information in this database. The central [Reaches](#reaches) table contains a single row for each reach segment in you stream network, while the supporting lookup tables contain all the parameters and labels needed to run the model. The [default values](https://github.com/Riverscapes/sqlBRAT/tree/master/database/data) are configured for the United States, but [international users]({{site.baseurl}}/Technical_Reference/international.html) can edit the lokup tables to configure it for their own needs.

![Database Schema](https://docs.google.com/drawings/d/e/2PACX-1vStWrxD4ZPoZqWN90m3BdNOpaAwT6rVSzUAUWixsePOJGKYWLKEOxgmHGkEhKx0o6ia_wxztdJVdZGi/pub?w=1021&h=807)

# Reaches

