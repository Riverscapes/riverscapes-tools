# Converting National Streamflow Statistics Database From Access to SQLite

The accompanying script `access_to_sqlite.py` converts the [National Streamflow Statistics](https://www.usgs.gov/software/national-streamflow-statistics-program-nss) database
from proprietary Microsoft Access format to open source SQLite. The code was adapted from a [publicly available GitHub gist](https://gist.github.com/snorfalorpagus/8578272) but required some tweaks (see below).

I ran this code on Windows. I have not tried it on any other operating system.

## Steps

1. Download and save the NSS Access database.

1. Open the database with **exclusive lock** and enter the password.

1. In Access go to File > Info and click the **Remove Database Password** option.

1. Perform the following modifications to the Access database to change tables and columns with reserved words:
    * Rename the table called `Parameters` as `Params`
    * Rename the columns in the `DetailedLog` table:
        * Rename column `Table` as `Tbl`
        * Rename column `Field` as `Fld`
    * Delete the table `Paste Errors` (if it exists).

1. Open your prefered Python IDE and ensure that you have [pyODBC](https://pypi.org/project/pyodbc/) installed.

1. Ensure that you are using a consistent bit architecture for Python, ODBC and Access driver. On my Windows system I knew that I had
the 32 bit version of Access installed. I checked the 32 ODBC drivers on my system and confirmed that I had a 32 bit Access driver (I did not have a 64 bit driver.) Then
I had to switch to a 32 bit version of Python (the one that comes with ArcGIS Desktop) instead of the default version that comes with PyCharm.

1. Run the `access_to_sqlite.py` script providing the existing Access file path as the first argument and the output path for a new SQLite database as the second argument.
The console should print each table name as it progresses.

## Outcome

The result is a rather crude SQLite database with no field types specified or relationships between tables. It's a fairly rough
straight dump of the schema and data. That said, it's sufficient for running simple SQL queries.

## Follow-On Research

Query to review the regional curves

```sql
SELECT State, R.RegionName, SL.StatLabel, DV.BCF, DV.Equation
FROM States S
    INNER JOIN Regions R ON S.StateCode = R.StateCode
    INNER JOIN DepVars DV ON R.RegionID = DV.RegionID
    INNER JOIN StatLabel SL ON DV.StatisticLabelID = SL.StatisticLabelID    
WHERE R.DataSourceID = '156'
    AND RegionName LIKE '%_Ann_%'
```

