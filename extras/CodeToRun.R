library(NcCorrelation)

options(andromedaTempFolder = "d:/andromedaTemp")
studyFolder <- "d:/NcCorrelation"
maxCores <- parallel::detectCores()


# Database-specific settings ---------------------------------------------------

# MDCD
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "redshift",
  connectionString = keyring::key_get("redShiftConnectionStringOhdaMdcd"),
  user = keyring::key_get("redShiftUserName"),
  password = keyring::key_get("redShiftPassword")
)
oracleTempSchema <- NULL
cdmDatabaseSchema <- "cdm_truven_mdcd_v2359"
cohortDatabaseSchema <- "scratch_mschuemi"
cohortTable <- "cohort_nc_correlation_mdcd"
outputFolder <- file.path(studyFolder, "Mdcd")
databaseId <- "MDCD"

# MDCR
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "redshift",
  connectionString = keyring::key_get("redShiftConnectionStringOhdaMdcr"),
  user = keyring::key_get("redShiftUserName"),
  password = keyring::key_get("redShiftPassword")
)
oracleTempSchema <- NULL
cdmDatabaseSchema <- "cdm_truven_mdcr_v2322"
cohortDatabaseSchema <- "scratch_mschuemi"
cohortTable <- "cohort_nc_correlation_mdcr"
outputFolder <- file.path(studyFolder, "Mdcr")
databaseId <- "MDCR"

# Optum EHR
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "redshift",
  connectionString = keyring::key_get("redShiftConnectionStringOhdaOptumEhr"),
  user = keyring::key_get("temp_user"),
  password = keyring::key_get("temp_password")
)
oracleTempSchema <- NULL
cdmDatabaseSchema <- "cdm_optum_ehr_v2137"
cohortDatabaseSchema <- "scratch_mschuemi"
cohortTable <- "cohort_nc_correlation_optum_ehr"
outputFolder <- file.path(studyFolder, "OptumEhr")
databaseId <- "Optum EHR"

# JMDC
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "redshift",
  connectionString = keyring::key_get("redShiftConnectionStringOhdaJmdc"),
  user = keyring::key_get("temp_user"),
  password = keyring::key_get("temp_password")
)
oracleTempSchema <- NULL
cdmDatabaseSchema <- "cdm_jmdc_v2432"
cohortDatabaseSchema <- "scratch_mschuemi"
cohortTable <- "cohort_nc_correlation_jmdc"
outputFolder <- file.path(studyFolder, "Jmdc")
databaseId <- "JMDC"


# Run analyses -----------------------------------------------------------------
execute(connectionDetails,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        maxCores = maxCores,
        minCellCount = 5,
        outputFolder = outputFolder,
        databaseId = databaseId,
        createCohorts = TRUE,
        runCohortMethod = TRUE,
        doBootstrap = TRUE,
        computeCorrelation = TRUE,
        exportForSharing = TRUE) 

# Generate plots and tables ----------------------------------------------------
# Should work even when csv files from multiple DBs are put in one export folder:
exportFolder <- file.path(outputFolder, "export")
plotFolder <- file.path(outputFolder, "plotsAndTables")
plotCorrelations(exportFolder = exportFolder,
                 plotFolder = plotFolder)
writeExtremeCorrelations(exportFolder = exportFolder,
                         plotFolder = plotFolder)
