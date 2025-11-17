CREATE TABLE Ecoregions (
     EcoregionID INTEGER PRIMARY KEY NOT NULL, 
     Name TEXT UNIQUE NOT NULL);

CREATE TABLE DamLimitations (
     LimitationID INTEGER PRIMARY KEY NOT NULL,
     Name TEXT UNIQUE NOT NULL);

CREATE TABLE DamOpportunities (
     OpportunityID INTEGER PRIMARY KEY NOT NULL, 
     Name TEXT UNIQUE NOT NULL);

CREATE TABLE DamRisks (
     RiskID INTEGER PRIMARY KEY NOT NULL, 
     Name TEXT UNIQUE NOT NULL);

CREATE TABLE DamCapacities (
     CapacityID INTEGER PRIMARY KEY NOT NULL, 
     Name TEXT UNIQUE NOT NULL, 
     MinCapacity REAL, 
     MaxCapacity REAL);
     
CREATE TABLE Epochs (
     EpochID INTEGER PRIMARY KEY NOT NULL, 
     Name TEXT NOT NULL UNIQUE, 
     Metadata TEXT, 
     Notes TEXT);

CREATE TABLE ReachCodes (
     ReachCode INTEGER PRIMARY KEY NOT NULL, 
     Name TEXT NOT NULL, 
     DisplayName TEXT, 
     Description TEXT NOT NULL);

CREATE TABLE ReachVegetation (
     ReachID INTEGER REFERENCES ReachAttributes ON DELETE CASCADE NOT NULL, 
     VegetationID INTEGER REFERENCES VegetationTypes (VegetationID) NOT NULL, 
     Buffer REAL NOT NULL CONSTRAINT CHK_ReachVegetation_Buffer CHECK (Buffer > 0), 
     Area REAL NOT NULL CONSTRAINT CHK_ReachVegetation_Area CHECK (Area > 0), 
     CellCount INTEGER NOT NULL CONSTRAINT CHK_ReachVegetation_CellCount CHECK (CellCount > 0), 
     PRIMARY KEY (ReachID, VegetationID, Buffer));

CREATE TABLE MetaData (
     KeyInfo TEXT PRIMARY KEY NOT NULL, 
     ValueInfo TEXT);

-- CREATE TABLE Agencies (
--      AgencyID INTEGER PRIMARY KEY NOT NULL UNIQUE, 
--      Name TEXT NOT NULL UNIQUE, 
--      Abbreviation TEXT NOT NULL UNIQUE);

CREATE TABLE VegetationOverrides (
     EcoregionID INTEGER REFERENCES Ecoregions (EcoregionID) ON DELETE CASCADE NOT NULL, 
     VegetationID INTEGER NOT NULL REFERENCES VegetationTypes (VegetationID) ON DELETE CASCADE, 
     OverrideSuitability INTEGER NOT NULL CONSTRAINT CHK_VegetationOverrides_Suitability CHECK (OverrideSuitability >= 0 AND OverrideSuitability <= 4), 
     Notes TEXT, 
     PRIMARY KEY (EcoregionID, VegetationID));

CREATE TABLE Watersheds (
     WatershedID TEXT PRIMARY KEY NOT NULL UNIQUE, 
     Name TEXT NOT NULL, 
     AreaSqKm REAL CONSTRAINT CHK_HUCs_Area CHECK (AreaSqKm >= 0), 
     States TEXT, 
     Geometry STRING, 
     EcoregionID INTEGER REFERENCES Ecoregions (EcoregionID), 
     QLow TEXT, 
     Q2 TEXT, 
     MaxDrainage REAL CHECK (MaxDrainage >= 0), 
     Metadata TEXT, 
     Notes TEXT);

CREATE TABLE VegetationTypes (
     VegetationID INTEGER PRIMARY KEY NOT NULL, 
     EpochID INTEGER REFERENCES Epochs (EpochID) NOT NULL, 
     Name TEXT NOT NULL, 
     DefaultSuitability INTEGER CONSTRAINT CHK_VegetationTypes_DefaultSuitability CHECK (DefaultSuitability >= 0 AND DefaultSuitability <= 4) NOT NULL DEFAULT (0), 
     LandUseID INTEGER REFERENCES LandUses (LandUseID), 
     Physiognomy TEXT, 
     Notes TEXT);

CREATE TABLE HydroAnthroDGO(
     DGOID INTEGER PRIMARY KEY NOT NULL,
     Slope REAL, 
     Length_m REAL, 
     DrainArea REAL, 
     QLow REAL, 
     Q2 REAL, 
     SPLow REAL,
     SP2 REAL,
     LUI REAL,
     Road_len REAL,
     Rail_len REAL,
     Canal_len REAL,
     RoaX_ct REAL,
     DivPts_ct REAL,
     Road_prim_len REAL,
     Road_sec_len REAL,
     Road_4wd_len REAL
);

CREATE TABLE ReachAttributes (
     ReachID INTEGER PRIMARY KEY NOT NULL,
     ReachCode INTEGER REFERENCES ReachCodes (ReachCode),
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
     IsPeren INTEGER NOT NULL DEFAULT (0), 
     iGeo_Slope REAL, 
     iGeo_Len REAL CONSTRAINT CHK_Reaches_LengthKm CHECK (iGeo_Len > 0), 
     iGeo_DA REAL CONSTRAINT CK_Reaches_DrainageAreaSqKm CHECK (iGeo_DA >= 0), 
     iVeg100EX REAL CONSTRAINT CHK_Reaches_ExistingVeg100 CHECK ((iVeg100EX >= 0) AND (iVeg100EX <= 4)), 
     iVeg_30EX REAL CONSTRAINT CHK_Reaches_ExistingVeg30 CHECK ((iVeg_30EX >= 0) AND (iVeg_30EX <= 4)), 
     iVeg100HPE REAL CONSTRAINT CHK_Reaches_HistoricVeg100 CHECK ((iVeg100HPE >= 0) AND (iVeg100HPE <= 4)), 
     iVeg_30HPE REAL CONSTRAINT CH_Reaches_HistoricVeg30 CHECK ((iVeg_30HPE >= 0) AND (iVeg_30HPE <= 4)), 
     iPC_Road REAL CONSTRAINT CHK_Reaches_RoadDist CHECK (iPC_Road >= 0), 
     iPC_RoadX REAL CONSTRAINT CHK_Reaches_RoadCrossDist CHECK (iPC_RoadX >= 0), 
     iPC_RoadVB REAL CONSTRAINT CGK_Reaches_RoadVBDist CHECK (iPC_RoadVB >= 0), 
     iPC_Rail REAL CONSTRAINT CHK_Reaches_RailDist CHECK (iPC_Rail >= 0), 
     iPC_RailVB REAL CONSTRAINT CHK_Reaches_RailVBDist CHECK (iPC_RailVB >= 0), 
     iPC_LU REAL, 
     iPC_VLowLU REAL, 
     iPC_LowLU REAL, 
     iPC_ModLU REAL, 
     iPC_HighLU REAL, 
     iHyd_QLow REAL CONSTRAINT CHK_Reaches_QLow CHECK (iHyd_QLow >= 0), 
     iHyd_Q2 REAL CONSTRAINT CHK_Reaches_Q2 CHECK (iHyd_Q2 >= 0), 
     iHyd_SPLow REAL CONSTRAINT CHK_Reaches_StreamPowerLow CHECK (iHyd_SPLow >= 0), 
     iHyd_SP2 REAL CONSTRAINT CHK_Reaches_StreamPower2 CHECK (iHyd_SP2 >= 0), 
     --AgencyID INTEGER REFERENCES Agencies (AgencyID), 
     oVC_HPE REAL, 
     oVC_EX REAL, 
     oCC_HPE REAL, 
     mCC_HPE_CT REAL, 
     oCC_EX REAL, 
     mCC_EX_CT REAL, 
     LimitationID INTEGER REFERENCES DamLimitations (LimitationID), 
     RiskID INTEGER REFERENCES DamRisks (RiskID), 
     OpportunityID INTEGER REFERENCES DamOpportunities (OpportunityID), 
     iPC_Canal REAL, 
     iPC_DivPts REAL, 
     iPC_Privat REAL, 
     oPC_Dist REAL, 
     IsMainCh INTEGER, 
     IsMultiCh INTEGER, 
     mCC_HisDep REAL,
     Dam_Setting TEXT);

CREATE INDEX ix_reach_attributes_level_path ON ReachAttributes(level_path);

CREATE TABLE DGOAttributes(
     DGOID INTEGER PRIMARY KEY NOT NULL, 
     WatershedID TEXT,
     ReachCode INTEGER REFERENCES ReachCodes (ReachCode),
     level_path REAL,
     seg_distance REAL,
     centerline_length REAL,
     segment_area REAL, 
     iGeo_DA REAL CONSTRAINT CK_DGO_DrainageAreaSqKm CHECK (iGeo_DA >= 0), 
     iGeo_Len REAL,
     iGeo_Slope REAL, 
     iVeg100EX REAL CONSTRAINT CHK_Reaches_ExistingVeg100 CHECK ((iVeg100EX >= 0) AND (iVeg100EX <= 4)),
     iVeg_30EX REAL CONSTRAINT CHK_Reaches_ExistingVeg30 CHECK ((iVeg_30EX >= 0) AND (iVeg_30EX <= 4)), 
     iVeg100HPE REAL CONSTRAINT CHK_Reaches_HistoricVeg100 CHECK ((iVeg100HPE >= 0) AND (iVeg100HPE <= 4)), 
     iVeg_30HPE REAL CONSTRAINT CH_Reaches_HistoricVeg30 CHECK ((iVeg_30HPE >= 0) AND (iVeg_30HPE <= 4)),
     iHyd_Q2 REAL CONSTRAINT CHK_Reaches_Q2 CHECK (iHyd_Q2 >= 0),
     iHyd_QLow REAL CONSTRAINT CHK_Reaches_QLow CHECK (iHyd_QLow >= 0),
     iHyd_SPLow REAL CONSTRAINT CHK_Reaches_StreamPowerLow CHECK (iHyd_SPLow >= 0),
     iHyd_SP2 REAL CONSTRAINT CHK_Reaches_StreamPower2 CHECK (iHyd_SP2 >= 0),
     oVC_HPE REAL, 
     oVC_EX REAL, 
     oCC_HPE REAL, 
     mCC_HPE_CT REAL, 
     oCC_EX REAL, 
     mCC_EX_CT REAL,
     LUI REAL,
     Road_len REAL,
     Rail_len REAL,
     Canal_len REAL,
     RoadX_ct REAL,
     DivPts_ct REAL,
     Road_prim_len REAL,
     Road_sec_len REAL,
     Road_4wd_len REAL,
     Risk TEXT,
     Limitation TEXT,
     Opportunity TEXT,
     Dam_Setting TEXT
     );

CREATE INDEX ix_dgo_attributes_level_path ON DGOAttributes(level_path, seg_distance);

CREATE TABLE IGOAttributes(
     IGOID INTEGER PRIMARY KEY NOT NULL,
     ReachCode INTEGER REFERENCES ReachCodes (ReachCode),
     level_path REAL,
     seg_distance REAL,
     stream_size INTEGER,
     oVC_EX REAL,
     oVC_HPE REAL,
     oCC_EX REAL,
     oCC_HPE REAL,
     Risk TEXT,
     Limitation TEXT,
     Opportunity TEXT,
     Dam_Setting TEXT
     );

CREATE INDEX ix_igo_attributes_level_path ON IGOAttributes(level_path, seg_distance);

-- I don't think we need the following because the compound primary key for ReachVegetation starts with ReachID
-- CREATE INDEX FK_ReachVegetation_ReachID ON ReachVegetation (ReachID);
CREATE INDEX FK_ReachVegetation_VegetationID ON ReachVegetation (VegetationID);
-- I don't think we need the following because the compound primary key for VegetationOverrides starts with EcoregionID
-- CREATE INDEX FK_VegetationOverrides_EcoregionID ON VegetationOverrides (EcoregionID);
CREATE INDEX FK_VegetationOverrides_VegetationID ON VegetationOverrides (VegetationID);
CREATE INDEX IX_Watersheds_EcoregionID ON Watersheds (EcoregionID);
CREATE INDEX IX_Watersheds_States ON Watersheds (States);
CREATE INDEX FK_VegetationTypes_EpochID ON VegetationTypes (EpochID);
CREATE INDEX IX_VegetationTypes_DefaultSuitability ON VegetationTypes (DefaultSuitability DESC);
CREATE INDEX FK_VegetationTypes_LandUseID ON VegetationTypes (LandUseID);
CREATE INDEX FX_Reaches_FCode ON ReachAttributes (ReachCode);
CREATE INDEX FX_Reaches_HUC ON ReachAttributes (WatershedID);
CREATE INDEX FX_Reaches_DamRiskID ON ReachAttributes (RiskID);
CREATE INDEX FX_Reaches_DamLimitationID ON ReachAttributes (LimitationID);
-- CREATE INDEX FX_Reaches_AgencyID ON ReachAttributes (AgencyID);
CREATE INDEX FX_Reaches_OpportunityID ON ReachAttributes (OpportunityID);

-- Non-spatial view of BRAT results with joins to the relevant tables
CREATE VIEW vwReachAttributes AS
SELECT R.*,
       -- W.Name         Watershed,
       RC.DisplayName ReachType,
       --A.Name         Agency,
       DL.Name        Limitation,
       DR.Name        Risk,
       DD.Name        Opportunity
FROM ReachAttributes R
         --INNER JOIN Watersheds W ON R.WatershedID = W.WatershedID
         LEFT JOIN ReachCodes RC ON R.ReachCode = RC.ReachCode
         --LEFT JOIN Agencies A ON R.AgencyID = A.AgencyID
         LEFT JOIN DamRisks DR ON R.RiskID = DR.RiskID
         LEFT JOIN DamLimitations DL ON R.LimitationID = DL.LimitationID
         LEFT JOIN DamOpportunities DD ON R.OpportunityID = DD.OpportunityID;

-- The main BRAT view that RAVE uses and end users should interact with
CREATE VIEW vwReaches AS SELECT R.*, G.geom
FROM vwReachAttributes R
         INNER JOIN ReachGeometry G ON R.ReachID = G.fid;

CREATE VIEW vwDgos AS SELECT D.*, G.geom
FROM DGOAttributes D
         INNER JOIN DGOGeometry G ON D.DGOID = G.DGOID;

CREATE VIEW vwIgos AS SELECT I.*, G.geom
FROM IGOAttributes I
         INNER JOIN IGOGeometry G ON I.IGOID = G.IGOID;

CREATE VIEW vwVegetationSuitability AS SELECT VT.VegetationID,
       VegetationName,
       EpochID,
       EpochName,
       VT.EcoregionID,
       EcoregionName,
       DefaultSuitability,
       OverrideSuitability,
       IFNULL(OverrideSuitability, DefaultSuitability) EffectiveSuitability
  FROM (
           SELECT VegetationID,
                  VT.Name VegetationName,
                  VT.EpochID,
                  Epochs.Name EpochName,
                  EcoregionID,
                  Ecoregions.Name EcoregionName,
                  DefaultSuitability
             FROM VegetationTypes VT
                  INNER JOIN
                  Epochs ON VT.EpochID = Epochs.EpochID
                  JOIN
                  Ecoregions
       )
       VT
       LEFT JOIN
       (
           SELECT EcoregionID,
                  VegetationID,
                  OverrideSuitability
             FROM VegetationOverrides
       )
       VO ON VT.VegetationID = VO.VegetationID AND 
             VT.EcoregionID = VO.EcoregionID
/* vwVegetationSuitability(VegetationID,VegetationName,EpochID,EpochName,EcoregionID,EcoregionName,DefaultSuitability,OverrideSuitability,EffectiveSuitability) */;
CREATE VIEW vwReachVegetationTypes AS SELECT E.EpochID,
       E.Name Epoch,
       RV.VegetationID,
       W.EcoregionID,
       VT.Name,
       Buffer,
       Round(SUM(Area), 0) TotalArea,
       VS.DefaultSuitability,
       VS.OverrideSuitability,
       VS.EffectiveSuitability
  FROM ReachVegetation RV
       INNER JOIN
       VegetationTypes VT ON RV.VegetationID = VT.VegetationID
       INNER JOIN
       Epochs E ON VT.EpochID = E.EpochID
       INNER JOIN
       ReachAttributes R ON RV.ReachID = R.ReachID
       INNER JOIN
       Watersheds W ON R.WatershedID = W.WatershedID
       INNER JOIN
       vwVegetationSuitability VS ON RV.VegetationID = VS.VegetationID AND 
                                     W.EcoregionID = VS.EcoregionID
 GROUP BY RV.VegetationID,
          Buffer,
          W.EcoregionID
 ORDER BY E.Name,
          TotalArea DESC;
/* vwReachVegetationTypes(EpochID,Epoch,VegetationID,Name,Buffer,TotalArea) */

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('Ecoregions', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DamLimitations', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DamOpportunities', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DamRisks', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DamCapacities', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('Epochs', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ReachCodes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ReachVegetation', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('MetaData', 'attributes');
--INSERT INTO gpkg_contents (table_name, data_type) VALUES ('Agencies', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('VegetationOverrides', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('Watersheds', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('VegetationTypes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('HydroAnthroDGO', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ReachAttributes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DGOAttributes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('IGOAttributes', 'attributes');

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('vwReachAttributes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('vwVegetationSuitability', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('vwReachVegetationTypes', 'attributes');