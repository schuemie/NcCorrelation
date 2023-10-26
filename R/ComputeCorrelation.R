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

computeCorrelation <- function(outputFolder, maxCores, databaseId) {
  results <- readRDS(file.path(outputFolder, "BootstrapEstimates.rds"))
  groups <- results %>% 
    group_by(.data$targetId, .data$comparatorId, .data$analysisId) %>%
    group_split()
  
  cluster <- ParallelLogger::makeCluster(min(maxCores, length(groups)))
  ParallelLogger::clusterRequire(cluster, "dplyr")
  on.exit(ParallelLogger::stopCluster(cluster))
  ParallelLogger::clusterApply(cluster = cluster, 
                               x = groups, 
                               fun = computeSingleCorrelationMatrix, 
                               outputFolder = outputFolder,
                               databaseId = databaseId)
}

# group = groups[[1]]
computeSingleCorrelationMatrix <- function(group, outputFolder, databaseId) {
  fileName <- file.path(outputFolder, 
                        sprintf("Correlations_t%d_c%d_a%d.rds",
                                group$targetId[1],
                                group$comparatorId[1],
                                group$analysisId[1],
                                databaseId))
  if (!file.exists(fileName)) {
    outcomes <- group %>%
      group_by(.data$outcomeId) %>%
      group_split()
    correlations <- matrix(0, length(outcomes), length(outcomes))
    names <- vector("integer", length(outcomes))
    for (i in seq(1, length(outcomes) - 1)) {
      names[i] <- outcomes[[i]]$outcomeId[1]
      for (j in seq(i+1, length(outcomes))) {
        logRr1 <- outcomes[[i]]$logRr
        logRr2 <- outcomes[[j]]$logRr
        idx <- !is.na(logRr1) & !is.na(logRr2)
        r <- cor(logRr1[idx], logRr2[idx])
        correlations[i, j] <- r
        correlations[j, i] <- r
      }
    }
    colnames(correlations) <- names
    rownames(correlations) <- names
    saveRDS(correlations, fileName)
  }
}
