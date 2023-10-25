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

computeCorrelation <- function(outputFolder, maxCores) {
  results <- readRDS(file.path(outputFolder, "BootstrapEstimates.rds"))
  groups <- results %>% 
    group_by(.data$targetId, .data$comparatorId, .data$analysisId) %>%
    group_split()
  
  cluster <- ParallelLogger::makeCluster(min(maxCores, length(groups)))
  ParallelLogger::clusterRequire(cluster, "dplyr")
  on.exit(ParallelLogger::stopCluster(cluster))
  matrices <- ParallelLogger::clusterApply(cluster = cluster, 
                                           x = groups, 
                                           fun = computeSingleCorrelationMatrix, 
                                           group = group,
                                           outputFolder = outputFolder,
                                           bootstrapFolder = bootstrapFolder)
}

# group = groups[[2]]
computeSingleCorrelationMatrix <- function(group) {
  outcomes <- group %>%
    group_by(.data$outcomeId) %>%
    group_split()
  correlations <- matrix(0, length(outcomes), length(outcomes))
  for (i in seq(1, length(outcomes) - 1)) {
    for (j in seq(i+1, length(outcomes))) {
      logRr1 <- outcomes[[i]]$logRr
      logRr2 <- outcomes[[j]]$logRr
      idx <- !is.na(logRr1) & !is.na(logRr2)
      r <- cor(logRr1[idx], logRr2[idx])
      correlations[i, j] <- r
      correlations[j, i] <- r
    }
  }
  meltedCorrelations <- reshape2::melt(correlations, na.rm = TRUE)
  library(ggplot2)
  ggplot(data = meltedCorrelations, aes(Var2, Var1, fill = value))+
    geom_tile(color = "white")+
    scale_fill_gradient2(low = "blue", high = "red", mid = "white", 
                         midpoint = 0, limit = c(-1,1), space = "Lab", 
                         name="Pearson\nCorrelation") +
    theme_minimal()+ 
    theme(axis.text.x = element_text(angle = 45, vjust = 1, 
                                     size = 12, hjust = 1))+
    coord_fixed()
  
  which(correlations > 0.75, arr.ind = TRUE) 
  correlations[19, 14]
  
  csvFileName <- system.file("NegativeControls.csv", package = "NcCorrelation") 
  negativeControls <- readr::read_csv(csvFileName, show_col_types = FALSE) 
  negativeControls[negativeControls$outcomeConceptId == outcomes[[19]]$outcomeId[1], ]
  negativeControls[negativeControls$outcomeConceptId == outcomes[[14]]$outcomeId[1], ]
}
