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

exportForSharing <- function(outputFolder, databaseId, minCellCount) {
  exportFolder <- file.path(outputFolder, "export")
  if (!dir.exists(exportFolder)) {
    dir.create(exportFolder)
  }

  # Correlation matrices
  fromFiles <- list.files(outputFolder, "Correlation.*.rds")
  toFiles <- gsub(".rds$", sprintf("_%s.csv", databaseId), fromFiles)
  for (i in seq_along(fromFiles)) {
    correlations <- readRDS(file.path(outputFolder, fromFiles[i]))
    write.csv(correlations, file.path(exportFolder, toFiles[i]))
  }

  # NC reference
  csvFileName <- system.file("NegativeControls.csv", package = "NcCorrelation")
  negativeControls <- readr::read_csv(csvFileName, show_col_types = FALSE)
  negativeControls <- negativeControls %>%
    rename(outcomeId = "outcomeConceptId") %>%
    select(-"targetConceptIds", -"comparatorConceptIds")
  readr::write_csv(negativeControls, file.path(exportFolder, "NegativeControlRef.csv"))

  # Analysis reference
  analysisRef <- tibble(
    analysisId = c(1, 2),
    description = c("Unadjusted", "PS stratified")
  )
  readr::write_csv(analysisRef, file.path(exportFolder, "AnalysisRef.csv"))

  # Hazard ratios
  estimates <- CohortMethod::getResultsSummary(outputFolder)
  estimates <- estimates %>%
    select(
      "analysisId",
      "targetId",
      "comparatorId",
      "outcomeId",
      "targetSubjects",
      "comparatorSubjects",
      "targetOutcomes", 
      "comparatorOutcomes",
      "ci95Lb",
      "ci95Ub",
      "logRr",
      "seLogRr"
    ) %>%
    mutate(
      targetSubjects = if_else(.data$targetSubjects < minCellCount, -minCellCount, .data$targetSubjects),
      comparatorSubjects = if_else(.data$comparatorSubjects < minCellCount, -minCellCount, .data$comparatorSubjects),
      targetOutcomes = if_else(.data$targetOutcomes < minCellCount, -minCellCount, .data$targetOutcomes),
      comparatorOutcomes = if_else(.data$comparatorOutcomes < minCellCount, -minCellCount, .data$comparatorOutcomes)
      ) %>%
    mutate(
      logRr = if_else(is.na(.data$seLogRr), NA, .data$logRr)
    )
  readr::write_csv(estimates, file.path(
    exportFolder,
    sprintf("HazardRatios_%s.csv", databaseId)
  ))

  # Systematic error distributions
  groups <- estimates %>%
    group_by(.data$targetId, .data$comparatorId, .data$analysisId) %>%
    group_split()
  distributions <- lapply(groups, computeSystematicErrorDistribution)
  distributions <- distributions %>%
    bind_rows(distributions)
  readr::write_csv(distributions, file.path(
    exportFolder,
    sprintf("SystematicError_%s.csv", databaseId)
  ))

  # Create zip
  zipName <- file.path(exportFolder, sprintf("Results_%s.zip", databaseId))
  files <- list.files(exportFolder, pattern = ".*\\.csv$")
  oldWd <- setwd(exportFolder)
  on.exit(setwd(oldWd))
  DatabaseConnector::createZipFile(zipFile = zipName, files = files)

  message("Results are ready for sharing at:", zipName)
}

# group = groups[[1]]
computeSystematicErrorDistribution <- function(group) {
  null <- suppressWarnings(EmpiricalCalibration::fitMcmcNull(group$logRr, group$seLogRr))
  ease <- EmpiricalCalibration::computeExpectedAbsoluteSystematicError(null)
  result <- group %>%
    head(1) %>%
    select("targetId", "comparatorId", "analysisId") %>%
    mutate(
      mean = null[1],
      sd = 1 / sqrt(null[2]),
      ease = ease$ease
    )
  return(result)
}
