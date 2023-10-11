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


doBootstrapping <- function(maxCores,
                            outputFolder,
                            nBootstraps = 100) {
  fileReference <- CohortMethod::getFileReference(outputFolder)
  groups <- fileReference %>%
    filter(.data$sharedPsFile != "") %>%
    distinct(.data$targetId, .data$comparatorId, .data$sharedPsFile, .data$outcomeId) %>%
    group_by(.data$targetId, .data$comparatorId, .data$sharedPsFile) %>%
    group_split()
  
  cluster <- ParallelLogger::makeCluster(max(maxCores, 10))
  on.exit(ParallelLogger::stopCluster(cluster))
  for (group in groups) {
    ParallelLogger::clusterApply(cluster = cluster, 
                                 x = seq_len(nBootstraps), 
                                 fun = doSingleBootstrap, 
                                 sharedPsFile = group$sharedPsFile[1], 
                                 outcomeIds = group$outcomeId,
                                 outputFolder = outputFolder)

    # Stratify by PS 
    
    # Sample with replacement
    
    # Per outcome: create study population and fit outcome model
    
    
  }
}

doSingleBootstrap <- function(seed, sharedPsFile, outcomeIds, outputFolder) {
  # population <- readRDS(file.path(outputFolder, sharedPsFile))
  # population <- CohortMethod::stratifyByPs(population, numberOfStrata = 10)
  # n <- nrow(strataPop)
  # population <- population[sample.int(n, n, replace = TRUE), ]
  # for (outcomeId in outcomeIds) {
  #   studyPop <- CohortMethod::createStudyPopulation()
  #   
  # }
  
}