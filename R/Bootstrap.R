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

#' @export
doBootstrapping <- function(maxCores,
                            outputFolder,
                            nBootstraps = 100) {
  bootstrapFolder <- file.path(outputFolder, "bootstrap")
  if (!dir.exists(bootstrapFolder))
    dir.create(bootstrapFolder)
  fileReference <- CohortMethod::getFileReference(outputFolder)
  groups <- fileReference %>%
    filter(.data$analysisId == 2) %>%
    select("targetId", "comparatorId", "outcomeId", "sharedPsFile", "studyPopFile") %>%
    group_by(.data$targetId, .data$comparatorId) %>%
    group_split()
  
  cluster <- ParallelLogger::makeCluster(min(maxCores, 10))
  ParallelLogger::clusterRequire(cluster, "dplyr")
  on.exit(ParallelLogger::stopCluster(cluster))
  for (group in groups) {
    message(sprintf("Bootstrapping for target ID %d and comparator ID %d",
                    group$targetId[1],
                    group$comparatorId[1],))
    ParallelLogger::clusterApply(cluster = cluster, 
                                 x = seq_len(nBootstraps), 
                                 fun = doSingleBootstrap, 
                                 group = group,
                                 outputFolder = outputFolder,
                                 bootstrapFolder = bootstrapFolder)
  }
}

# group = groups[[1]]
doSingleBootstrap <- function(iteration, group, outputFolder, bootstrapFolder) {
  fileName <- file.path(bootstrapFolder, sprintf("Estimates_t%d_c%d_i%d.rds",
                                              group$targetId[1],
                                              group$comparatorId[1],
                                              iteration))
  if (!file.exists(fileName)) {
    set.seed(iteration)
    population <- readRDS(file.path(outputFolder, group$sharedPsFile[1]))
    
    # Stratify by PS
    population <- CohortMethod::stratifyByPs(population, numberOfStrata = 10)
    
    # Sample with replacement
    n <- nrow(population)
    population <- population[sample.int(n, n, replace = TRUE), ]
    
    # Create study populations and fit outcome models
    estimates <- vector("list", length = nrow(group))
    for (i in seq_len(nrow(group))) {
      # Loading original study population and joining with that. Assuming that is
      # just as fast as loading the outcome table and joining with that, but this
      # is simpler
      studyPop <- readRDS(file.path(outputFolder, group$studyPopFile[i]))
      studyPop <- population %>%
        select("rowId", "stratumId") %>%
        inner_join(studyPop, by = join_by("rowId"))
      # if (!all(studyPop$rowId == population$rowId)) {
      #   stop("Error joining tables")
      # }
      studyPop$rowId <- seq_along(studyPop$rowId)
      adjustedModel <- CohortMethod::fitOutcomeModel(
        population = studyPop,
        modelType = "cox",
        stratified = TRUE,
        profileBounds = NULL
      )
      if (is.null(adjustedModel$outcomeModelTreatmentEstimate)) {
        adjustedModel$outcomeModelTreatmentEstimate <- tibble(logRr = NA,
                                                              logLb95 = NA,
                                                              logUb95 = NA,
                                                              seLogRr = NA,
                                                              llr = NA)
      }
      unAdjustedModel <- CohortMethod::fitOutcomeModel(
        population = studyPop,
        modelType = "cox",
        stratified = FALSE,
        profileBounds = NULL
      )
      if (is.null(unAdjustedModel$outcomeModelTreatmentEstimate)) {
        unAdjustedModel$outcomeModelTreatmentEstimate <- tibble(logRr = NA,
                                                                logLb95 = NA,
                                                                logUb95 = NA,
                                                                seLogRr = NA,
                                                                llr = NA)
      }
      estimates[[i]] <- bind_rows(unAdjustedModel$outcomeModelTreatmentEstimate,
                                  adjustedModel$outcomeModelTreatmentEstimate) %>%
        mutate(analysisId = c(1, 2),
               outcomeId = group$outcomeId[1])
    }
    estimates <- estimates %>%
      bind_rows() %>%
      mutate(targetId = group$targetId[1],
             comparatorId = group$comparatorId[1],
             iteration = !!iteration)
    saveRDS(estimates, fileName)
  }
}