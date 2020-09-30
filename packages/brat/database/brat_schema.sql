CREATE TABLE Ecoregions (EcoregionID INTEGER PRIMARY KEY UNIQUE NOT NULL, Name TEXT UNIQUE NOT NULL);
CREATE TABLE DamLimitations (LimitationID INTEGER PRIMARY KEY NOT NULL UNIQUE, Name TEXT UNIQUE NOT NULL);
CREATE TABLE DamOpportunities (OpportunityID INTEGER PRIMARY KEY UNIQUE NOT NULL, Name TEXT UNIQUE NOT NULL);
CREATE TABLE DamRisks (RiskID INTEGER PRIMARY KEY UNIQUE NOT NULL, Name TEXT UNIQUE NOT NULL);
CREATE TABLE DamCapacities (CapacityID INTEGER PRIMARY KEY NOT NULL, Name TEXT UNIQUE NOT NULL, MinCapacity REAL, MaxCapacity REAL);
CREATE TABLE Epochs (EpochID INTEGER PRIMARY KEY NOT NULL, Name TEXT NOT NULL UNIQUE, Metadata TEXT, Notes TEXT);
CREATE TABLE ReachCodes (ReachCode INTEGER PRIMARY KEY NOT NULL, Name TEXT NOT NULL, DisplayName TEXT, Description TEXT NOT NULL);
CREATE TABLE ReachVegetation (ReachID INTEGER REFERENCES Reaches ON DELETE CASCADE NOT NULL, VegetationID INTEGER REFERENCES VegetationTypes (VegetationID) NOT NULL, Buffer REAL NOT NULL CONSTRAINT CHK_ReachVegetation_Buffer CHECK (Buffer > 0), Area REAL NOT NULL CONSTRAINT CHK_ReachVegetation_Area CHECK (Area > 0), CellCount REAL NOT NULL CONSTRAINT CHK_ReachVegetation_CellCount CHECK (CellCount > 0), PRIMARY KEY (ReachID, VegetationID, Buffer));
CREATE TABLE MetaData (KeyInfo TEXT PRIMARY KEY NOT NULL, ValueInfo TEXT);
CREATE TABLE LandUses (LandUseID INTEGER PRIMARY KEY NOT NULL, Name TEXT UNIQUE NOT NULL, Intensity REAL NOT NULL CONSTRAINT CHK_LandUses_Itensity CHECK (Intensity >= 0 AND Intensity <= 1) DEFAULT (0));
CREATE TABLE Agencies (AgencyID INTEGER PRIMARY KEY NOT NULL UNIQUE, Name TEXT NOT NULL UNIQUE, Abbreviation TEXT NOT NULL UNIQUE);
CREATE TABLE LandUseIntensities (IntensityID INTEGER PRIMARY KEY NOT NULL, Name TEXT UNIQUE NOT NULL, MaxIntensity REAL NOT NULL UNIQUE, TargetCol TEXT UNIQUE NOT NULL);
CREATE TABLE VegetationOverrides (EcoregionID INTEGER REFERENCES Ecoregions (EcoregionID) ON DELETE CASCADE NOT NULL, VegetationID INTEGER NOT NULL REFERENCES VegetationTypes (VegetationID) ON DELETE CASCADE, OverrideSuitability INTEGER NOT NULL CONSTRAINT CHK_VegetationOverrides_Suitability CHECK (OverrideSuitability >= 0 AND OverrideSuitability <= 4), Notes TEXT, PRIMARY KEY (EcoregionID, VegetationID));
CREATE TABLE WatershedHydroParams (WatershedID TEXT REFERENCES Watersheds (WatershedID) ON DELETE CASCADE NOT NULL, ParamID INTEGER REFERENCES HydroParams (ParamID) NOT NULL, Value REAL NOT NULL, PRIMARY KEY (WatershedID, ParamID));
CREATE TABLE Watersheds (WatershedID TEXT PRIMARY KEY NOT NULL UNIQUE, Name TEXT NOT NULL, AreaSqKm REAL CONSTRAINT CHK_HUCs_Area CHECK (AreaSqKm >= 0), States TEXT, Geometry STRING, EcoregionID INTEGER REFERENCES Ecoregions (EcoregionID), QLow TEXT, Q2 TEXT, MaxDrainage REAL CHECK (MaxDrainage >= 0), Metadata TEXT, Notes TEXT);
CREATE TABLE VegetationTypes (VegetationID INTEGER PRIMARY KEY NOT NULL, EpochID INTEGER REFERENCES Epochs (EpochID) NOT NULL, Name TEXT NOT NULL, DefaultSuitability INTEGER CONSTRAINT CHK_VegetationTypes_DefaultSuitability CHECK (DefaultSuitability >= 0 AND DefaultSuitability <= 4) NOT NULL DEFAULT (0), LandUseID INTEGER REFERENCES LandUses (LandUseID), Physiognomy TEXT, Notes TEXT);
CREATE TABLE Reaches (ReachID INTEGER PRIMARY KEY NOT NULL, WatershedID TEXT REFERENCES Watersheds (WatershedID) ON DELETE CASCADE, Geometry TEXT, ReachCode INTEGER REFERENCES ReachCodes (ReachCode), IsPeren INTEGER NOT NULL DEFAULT (0), StreamName TEXT, Orig_DA REAL, iGeo_Slope REAL, iGeo_ElMax REAL, iGeo_ElMin REAL, iGeo_Len REAL CONSTRAINT CHK_Reaches_LengthKm CHECK (iGeo_Len > 0), iGeo_DA REAL CONSTRAINT CK_Reaches_DrainageAreaSqKm CHECK (iGeo_DA >= 0), iVeg100EX REAL CONSTRAINT CHK_Reaches_ExistingVeg100 CHECK ((iVeg100EX >= 0) AND (iVeg100EX <= 4)), iVeg_30EX REAL CONSTRAINT CHK_Reaches_ExistingVeg30 CHECK ((iVeg_30EX >= 0) AND (iVeg_30EX <= 4)), iVeg100HPE REAL CONSTRAINT CHK_Reaches_HistoricVeg100 CHECK ((iVeg100HPE >= 0) AND (iVeg100HPE <= 4)), iVeg_30HPE REAL CONSTRAINT CH_Reaches_HistoricVeg30 CHECK ((iVeg_30HPE >= 0) AND (iVeg_30HPE <= 4)), iPC_Road REAL CONSTRAINT CHK_Reaches_RoadDist CHECK ((iPC_Road >= 0)), iPC_RoadX REAL CONSTRAINT CHK_Reaches_RoadCrossDist CHECK (iPC_RoadX >= 0), iPC_RoadVB REAL CONSTRAINT CGK_Reaches_RoadVBDist CHECK (iPC_RoadVB >= 0), iPC_Rail REAL CONSTRAINT CHK_Reaches_RailDist CHECK (iPC_Rail >= 0), iPC_RailVB REAL CONSTRAINT CHK_Reaches_RailVBDist CHECK (iPC_RailVB >= 0), iPC_LU REAL, iPC_VLowLU REAL, iPC_LowLU REAL, iPC_ModLU REAL, iPC_HighLU REAL, iHyd_QLow REAL CONSTRAINT CHK_Reaches_QLow CHECK (iHyd_QLow >= 0), iHyd_Q2 REAL CONSTRAINT CHK_Reaches_Q2 CHECK (iHyd_Q2 >= 0), iHyd_SPLow REAL CONSTRAINT CHK_Reaches_StreamPowerLow CHECK (iHyd_SPLow >= 0), iHyd_SP2 REAL CONSTRAINT CHK_Reaches_StreamPower2 CHECK (iHyd_SP2 >= 0), AgencyID INTEGER REFERENCES Agencies (AgencyID), oVC_HPE REAL, oVC_EX REAL, oCC_HPE REAL, mCC_HPE_CT REAL, oCC_EX REAL, mCC_EX_CT REAL, LimitationID INTEGER REFERENCES DamLimitations (LimitationID), RiskID INTEGER REFERENCES DamRisks (RiskID), OpportunityID INTEGER REFERENCES DamOpportunities (OpportunityID), iPC_Canal REAL, iPC_DivPts REAL, iPC_Privat REAL, oPC_Dist REAL, IsMainCh INTEGER, IsMultiCh INTEGER, mCC_HisDep REAL);
CREATE TABLE HydroParams (ParamID INTEGER PRIMARY KEY NOT NULL, Name TEXT UNIQUE NOT NULL, Description TEXT NOT NULL, Aliases TEXT, DataUnits TEXT NOT NULL, EquationUnits TEXT, Conversion REAL NOT NULL DEFAULT (1), Definition TEXT);
CREATE INDEX FK_ReachVegetation_ReachID ON ReachVegetation (ReachID);
CREATE INDEX FK_ReachVegetation_VegetationID ON ReachVegetation (VegetationID);
CREATE INDEX FK_VegetationOverrides_EcoregionID ON VegetationOverrides (EcoregionID);
CREATE INDEX FK_VegetationOverrides_VegetationID ON VegetationOverrides (VegetationID);
CREATE INDEX IX_Watersheds_EcoregionID ON Watersheds (EcoregionID);
CREATE INDEX IX_Watersheds_States ON Watersheds (States);
CREATE INDEX FK_VegetationTypes_EpochID ON VegetationTypes (EpochID);
CREATE INDEX IX_VegetationTypes_DefaultSuitability ON VegetationTypes (DefaultSuitability DESC);
CREATE INDEX FK_VegetationTypes_LandUseID ON VegetationTypes (LandUseID);
CREATE INDEX FX_Reaches_FCode ON Reaches (ReachCode);
CREATE INDEX FX_Reaches_HUC ON Reaches (WatershedID);
CREATE INDEX FX_Reaches_DamRiskID ON Reaches (RiskID);
CREATE INDEX FX_Reaches_DamLimitationID ON Reaches (LimitationID);
CREATE INDEX FX_Reaches_AgencyID ON Reaches (AgencyID);
CREATE INDEX FX_Reaches_OpportunityID ON Reaches (OpportunityID);
CREATE VIEW vwReaches AS SELECT R.*, W.Name Watershed, RC.DisplayName ReachType, A.Name Agency, DL.Name Limitation, DR.Name Risk, DD.Name Opportunity FROM Reaches R INNER JOIN Watersheds W ON R.WatershedID = W.WatershedID LEFT JOIN ReachCodes RC ON R.ReachCode = RC.ReachCode LEFT JOIN Agencies A ON R.AgencyID = A.AgencyID LEFT JOIN DamRisks DR ON R.RiskID = DR.RiskID LEFT JOIN DamLimitations DL ON R.LimitationID = DL.LimitationID LEFT JOIN DamOpportunities DD ON R.OpportunityID = DD.OpportunityID
/* vwReaches(ReachID,WatershedID,Geometry,ReachCode,IsPeren,StreamName,Orig_DA,iGeo_Slope,iGeo_ElMax,iGeo_ElMin,iGeo_Len,iGeo_DA,iVeg100EX,iVeg_30EX,iVeg100HPE,iVeg_30HPE,iPC_Road,iPC_RoadX,iPC_RoadVB,iPC_Rail,iPC_RailVB,iPC_LU,iPC_VLowLU,iPC_LowLU,iPC_ModLU,iPC_HighLU,iHyd_QLow,iHyd_Q2,iHyd_SPLow,iHyd_SP2,AgencyID,oVC_HPE,oVC_EX,oCC_HPE,mCC_HPE_CT,oCC_EX,mCC_EX_CT,LimitationID,RiskID,OpportunityID,iPC_Canal,iPC_DivPts,iPC_Privat,oPC_Dist,IsMainCh,IsMultiCh,mCC_HisDep,Watershed,ReachType,Agency,Limitation,Risk,Opportunity) */;
CREATE VIEW vwHydroParams AS SELECT W.WatershedID,
       W.Name AS Watershed,
       W.States,
       W.Metadata,
       E.EcoregionID,
       E.Name AS Ecoregion,
       HP.ParamID,
       HP.Name AS Parameter,
       HP.Aliases,
       HP.DataUnits,
       HP.EquationUnits,
       WHP.Value,
       HP.Conversion,
       WHP.Value * HP.Conversion AS ConvertedValue
  FROM Watersheds W
       INNER JOIN
       Ecoregions E ON W.EcoregionID = E.EcoregionID
       INNER JOIN
       WatershedHydroParams WHP ON W.WatershedID = WHP.WatershedID
       INNER JOIN
       HydroParams HP ON WHP.ParamID = HP.ParamID
/* vwHydroParams(WatershedID,Watershed,States,Metadata,EcoregionID,Ecoregion,ParamID,Parameter,Aliases,DataUnits,EquationUnits,Value,Conversion,ConvertedValue) */;
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
       Reaches R ON RV.ReachID = R.ReachID
       INNER JOIN
       Watersheds W ON R.WatershedID = W.WatershedID
       INNER JOIN
       vwVegetationSuitability VS ON RV.VegetationID = VS.VegetationID AND 
                                     W.EcoregionID = VS.EcoregionID
 GROUP BY RV.VegetationID,
          Buffer,
          W.EcoregionID
 ORDER BY E.Name,
          TotalArea DESC
/* vwReachVegetationTypes(EpochID,Epoch,VegetationID,Name,Buffer,TotalArea) */
