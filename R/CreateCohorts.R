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
# library(dplyr)

#' @export
createCohorts <- function(connectionDetails,
                          cdmDatabaseSchema,
                          cohortDatabaseSchema,
                          cohortTable) {
  connection <- connect(connectionDetails)
  on.exit(disconnect(connection))
  
  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable)
  CohortGenerator::createCohortTables(
    connection = connection, 
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames
  )
  # Generate exposure cohorts -------------------------------------------------
  rdsFileName <- system.file("CohortDefinitionSet.rds", package = "NcCorrelation")
  cohortDefinitionSet <- readRDS(rdsFileName)
  CohortGenerator::generateCohortSet(
    connection = connection, 
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet
  )
  # Generate negative control outcome cohorts ---------------------------------
  csvFileName <- system.file("NegativeControls.csv", package = "NcCorrelation")
  negativeControlOutcomeCohortSet <- readr::read_csv(csvFileName, show_col_types = FALSE) %>%
    mutate(cohortId = .data$outcomeConceptId) %>%
    select("cohortId", cohortName = "outcomeName", "outcomeConceptId")
  CohortGenerator::generateNegativeControlOutcomeCohorts(
    connection = connection, 
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cdmDatabaseSchema = cdmDatabaseSchema,
    negativeControlOutcomeCohortSet = negativeControlOutcomeCohortSet,
    occurrenceType = "first",
    detectOnDescendants = TRUE
  )
}
