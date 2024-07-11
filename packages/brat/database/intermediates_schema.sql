CREATE TABLE HydroAnthroReach(
     ReachID INTEGER PRIMARY KEY NOT NULL,
     Slope REAL, 
     Length_m REAL, 
     DrainArea REAL, 
     QLow REAL, 
     Q2 REAL, 
     SPLow REAL, 
     SP2 REAL, 
     iPC_Road REAL, 
     iPC_RoadX REAL, 
     iPC_RoadVB REAL, 
     iPC_Rail REAL, 
     iPC_RailVB REAL, 
     iPC_DivPts REAL, 
     iPC_Privat REAL, 
     iPC_Canal REAL, 
     iPC_LU REAL, 
     iPC_VLowLU REAL, 
     iPC_LowLU REAL, 
     iPC_ModLU REAL, 
     iPC_HighLU REAL, 
     oPC_Dist REAL
);

CREATE TABLE ReachAttributes(
     ReachID INTEGER PRIMARY KEY NOT NULL,
     FCode TEXT,
     StreamName TEXT,
     NHDPlusID REAL,
     WatershedID TEXT,
     level_path REAL,
     ownership TEXT,
     divergence REAL,
     stream_order INTEGER,
     us_state TEXT,
     ecoregion_iii TEXT,
     ecoregion_iv TEXT,
     iGeo_Slope REAL,
     iGeo_Len REAL,
     iGeo_DA REAL,
     iHyd_Q2 REAL,
     iHyd_QLow REAL,
     iHyd_SP2 REAL,
     iHyd_SPLow REAL,
     iPC_Road REAL, 
     iPC_RoadX REAL, 
     iPC_RoadVB REAL, 
     iPC_Rail REAL, 
     iPC_RailVB REAL, 
     iPC_DivPts REAL, 
     iPC_Privat REAL, 
     iPC_Canal REAL, 
     iPC_LU REAL, 
     iPC_VLowLU REAL, 
     iPC_LowLU REAL, 
     iPC_ModLU REAL, 
     iPC_HighLU REAL, 
     oPC_Dist REAL
);

CREATE VIEW vwIntReaches AS SELECT R.*, G.geom
FROM ReachAttributes R
INNER JOIN attributed_network G ON R.ReachID = G.ReachID;

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('HydroAnthroReach', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ReachAttributes', 'attributes');