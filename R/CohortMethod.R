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

runCohortMethod <- function(connectionDetails,
                            cdmDatabaseSchema,
                            cohortDatabaseSchema,
                            cohortTable,
                            maxCores,
                            outputFolder) {
  # Create list of target-comparator-outcomes ---------------------------------
  csvFileName <- system.file("NegativeControls.csv", package = "NcCorrelation")
  negativeControls <- readr::read_csv(csvFileName, show_col_types = FALSE) %>%
    group_by(.data$targetId, .data$comparatorId) %>%
    group_split()
  
  tcosList <- list()
  for (i in seq_along(negativeControls)) {
    controls <- negativeControls[[i]]
    excludedCovariateConceptIds <- c(
      as.numeric(strsplit(as.character(controls$targetConceptIds[1]), ";")[[1]]),
      as.numeric(strsplit(as.character(controls$comparatorConceptIds[1]), ";")[[1]])
    )
    outcomes <- list()
    for (j in seq_along(controls$outcomeConceptId)) {
      outcomes[[j]] <- CohortMethod::createOutcome(
        outcomeId = controls$outcomeConceptId[j],
        outcomeOfInterest = TRUE
      )
    }
    tcos <- CohortMethod::createTargetComparatorOutcomes(
      targetId = controls$targetId[1],
      comparatorId = controls$comparatorId[1],
      outcomes = outcomes,
      excludedCovariateConceptIds = excludedCovariateConceptIds
    )
    tcosList[[i]] <- tcos
  }
  
  # Create analysis settings list -------------------------------------------------
  covariateSettings <- FeatureExtraction::createDefaultCovariateSettings(addDescendantsToExclude = TRUE)
  getDbCmDataArgs <- CohortMethod::createGetDbCohortMethodDataArgs(
    removeDuplicateSubjects = "remove all",
    maxCohortSize = 1e6,
    covariateSettings = covariateSettings
  )
  createStudyPopArgs <- CohortMethod::createCreateStudyPopulationArgs(
    removeSubjectsWithPriorOutcome = TRUE,
    minDaysAtRisk = 1,
    riskWindowStart = 0,
    startAnchor = "cohort start",
    riskWindowEnd = 0,
    endAnchor = "cohort end"
  )
  createPsArgs <- CohortMethod::createCreatePsArgs(
    errorOnHighCorrelation = TRUE,
    stopOnError = FALSE,
    maxCohortSizeForFitting = 150000,
    control = Cyclops::createControl(
      cvType = "auto",
      startingVariance = 0.01,
      noiseLevel = "quiet",
      tolerance = 2e-07,
      cvRepetitions = 10,
      minCVData = 10
    )
  )
  fitOutcomeModelArgs1 <- CohortMethod::createFitOutcomeModelArgs(modelType = "cox", stratified = FALSE)
  cmAnalysis1 <- CohortMethod::createCmAnalysis(
    analysisId = 1,
    description = "Unadjusted",
    getDbCohortMethodDataArgs = getDbCmDataArgs,
    createStudyPopArgs = createStudyPopArgs,
    fitOutcomeModelArgs = fitOutcomeModelArgs1
  )
  stratifyByPsArgs <- CohortMethod::createStratifyByPsArgs(numberOfStrata = 10)
  fitOutcomeModelArgs2 <- CohortMethod::createFitOutcomeModelArgs(modelType = "cox", stratified = TRUE)
  computeSharedCovariateBalanceArgs <- CohortMethod::createComputeCovariateBalanceArgs()
  cmAnalysis2 <- CohortMethod::createCmAnalysis(
    analysisId = 2,
    description = "PS stratification",
    getDbCohortMethodDataArgs = getDbCmDataArgs,
    createStudyPopArgs = createStudyPopArgs,
    createPsArgs = createPsArgs,
    stratifyByPsArgs = stratifyByPsArgs,
    computeSharedCovariateBalanceArgs = computeSharedCovariateBalanceArgs,
    fitOutcomeModelArgs = fitOutcomeModelArgs2
  )
  cmAnalysisList <- list(cmAnalysis1, cmAnalysis2)
  
  # Run analyses ----------------------------------------------------------------------
  multiThreadingSettings <- CohortMethod::createMultiThreadingSettings(
    getDbCohortMethodDataThreads = 1,
    createPsThreads = max(1, floor(maxCores / 10)),
    psCvThreads = min(10, maxCores),
    createStudyPopThreads = min(3, maxCores),
    trimMatchStratifyThreads = min(5, maxCores),
    computeSharedBalanceThreads = min(3, maxCores),
    computeBalanceThreads = min(5, maxCores),
    prefilterCovariatesThreads = min(3, maxCores),
    fitOutcomeModelThreads = max(1, floor(maxCores / 4)),
    outcomeCvThreads = min(4, maxCores),
    calibrationThreads = min(4, maxCores)
  )
  CohortMethod::runCmAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    exposureDatabaseSchema = cohortDatabaseSchema,
    exposureTable = cohortTable,
    outcomeDatabaseSchema = cohortDatabaseSchema,
    outcomeTable = cohortTable,
    outputFolder = outputFolder,
    cmAnalysisList = cmAnalysisList,
    targetComparatorOutcomesList = tcosList,
    multiThreadingSettings = multiThreadingSettings
  )
}
