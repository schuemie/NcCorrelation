# Copyright 2023 Observational Health Data Sciences and Informatics
#
# This file is part of NcCorrelation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Run the main analysis
#'
#' @param connectionDetails       An R object of type \code{ConnectionDetails} created using the
#'                                function \code{createConnectionDetails} in the
#'                                \code{DatabaseConnector} package.
#' @param cdmDatabaseSchema       A database schema containing health care data in the OMOP Commond
#'                                Data Model. Note that for SQL Server, both the database and schema
#'                                should be specified, e.g. 'cdm_schema.dbo'.
#' @param cohortDatabaseSchema    The name of the database schema where the exposure and outcome cohorts will be
#'                                created. 
#' @param cohortTable             The name of the table that will be created to store the exposure
#'                                and outcome cohorts. 
#' @param maxCores                How many parallel cores should be used? If more cores are made available
#'                                this can speed up the analyses.
#' @param outputFolder            Name of local folder to place intermediary results; make sure to use
#'                                forward slashes (/). Do not use a folder on a network drive since
#'                                this greatly impacts performance.
#' @param databaseId              A string used to identify the database in the results. Will be used
#'                                to generate file names, so avoid special characters.
#' @param createCohorts           Should the cohorts be created? If `FALSE`, the cohorts are assumed to already
#'                                exist.
#' @param runCohortMethod         Run CohortMethod to produce effect-size estimates?                                
#' @param doBootstrap             Perform the bootstrap? Requires CohortMethod to have completed.
#' @param computeCorrelation      Compute correlation between negative control estimates? Requires
#'                                the bootstrap to have completed.
#' @param exportForSharing        Export results to CSV files for sharing?
#'
#' @export
execute <- function(connectionDetails,
                    cdmDatabaseSchema,
                    cohortDatabaseSchema,
                    cohortTable,
                    maxCores = 1,
                    outputFolder,
                    databaseId,
                    createCohorts = TRUE,
                    runCohortMethod = TRUE,
                    doBootstrap = TRUE,
                    computeCorrelation = TRUE,
                    exportForSharing = TRUE) {
  if (!file.exists(outputFolder)) {
    dir.create(outputFolder, recursive = TRUE)
  }
  
  ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
  ParallelLogger::addDefaultErrorReportLogger(file.path(outputFolder, "errorReportR.txt"))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE), add = TRUE)
  if (createCohorts) {
    message("Creating cohorts")
    createCohorts(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable
    )
  }
  if (runCohortMethod) {
    message("Running cohort method")
    runCohortMethod(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      outputFolder = outputFolder,
      maxCores = maxCores
    )
  }
  if (doBootstrap) {
    message("Performing bootstrap")
    doBootstrapping(
      maxCores = maxCores,
      outputFolder = outputFolder
    )
  }
  if (computeCorrelation) {
    message("Computing correlations")
    computeCorrelation(outputFolder = outputFolder, 
                       maxCores = maxCores) 
  }
  if (exportForSharing) {
    message("Exporting results")
    exportForSharing(outputFolder = outputFolder,
                     databaseId = databaseId)
  }
}
