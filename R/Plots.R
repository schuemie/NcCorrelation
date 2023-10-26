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

# plotFolder <- file.path(outputFolder, "plotsAndTables")

#' Plot correlation matrices
#'
#' @param exportFolder The folder containing the CSV files.
#' @param plotFolder   The folder where the plots will be written.
#'
#' @return
#' Does not return anything. Is called for the side-effect of having plot files written.
#' 
#' @export
plotCorrelations <- function(exportFolder, plotFolder) { 
  if (!dir.exists(plotFolder)) {
    dir.create(plotFolder)
  }
  files <- list.files(exportFolder, "Correlations.*.csv")
  # file = files[1]
  for (file in files) {
    correlations <- as.matrix(read.csv(file.path(exportFolder, file), row.names = 1, header = TRUE))
    colnames(correlations) <- gsub("^X", "", colnames(correlations))
    
    colnames(correlations) <- seq_len(ncol(correlations))
    rownames(correlations) <- seq_len(nrow(correlations))
    vizData <- as_tibble(expand.grid(x = seq_len(ncol(correlations)), y = seq_len(ncol(correlations)))) %>%
      mutate(rho = c(correlations)) %>%
      filter(!is.na(.data$rho))
    plot <- ggplot2::ggplot(data = vizData, ggplot2::aes(x = .data$x, y = .data$y, fill = .data$rho)) +
      ggplot2::geom_tile() +
      ggplot2::scale_fill_gradient2(low = "blue", 
                                    high = "red", 
                                    mid = "white", 
                                    midpoint = 0, 
                                    limit = c(-1,1), 
                                    name = "Pearson\nCorrelation") +
      ggplot2::theme(axis.title = ggplot2::element_blank(),
                     axis.text = ggplot2::element_blank(),
                     axis.ticks = ggplot2::element_blank(),
                     panel.background = ggplot2::element_blank()) +
      ggplot2::coord_fixed()
    fileName <- file.path(plotFolder, gsub(".csv", ".png", file))
    ggplot2::ggsave(filename = fileName, plot = plot, width = 8, height = 7, dpi = 200)
  }
}

#' Write most extreme correlations to file
#'
#' @param exportFolder The folder containing the CSV files.
#' @param plotFolder   The folder where the table will be written.
#'
#' @return
#' Does not return anything. Is called for the side-effect of having a CSV file written.
#' 
#' @export
writeExtremeCorrelations <- function(exportFolder, plotFolder) { 
  if (!dir.exists(plotFolder)) {
    dir.create(plotFolder)
  }
  csvFileName <- system.file("NegativeControls.csv", package = "NcCorrelation")
  negativeControls <- readr::read_csv(csvFileName, show_col_types = FALSE)
  outcomes <- negativeControls %>%
    distinct(.data$outcomeConceptId, .data$outcomeName) %>%
    rename(outcomeId = "outcomeConceptId")
  files <- list.files(exportFolder, "Correlations.*.csv")
  allResults <- list()
  # file = files[1]
  for (file in files) {
    correlations <- as.matrix(read.csv(file.path(exportFolder, file), row.names = 1, header = TRUE))
    colnames(correlations) <- gsub("^X", "", colnames(correlations))
    
    idx <- which(abs(correlations) > 0.5, arr.ind = TRUE) 
    idx <- idx[idx[, 1] < idx[, 2], ]
    results <- list()
    for (i in seq_len(nrow(idx))) {
      rowIdx <- idx[i, 1]
      colIdx <- idx[i, 2]
      results[[i]] <- tibble(
        outcomeId1 = as.numeric(rownames(correlations)[rowIdx]),
        outcomeId2 = as.numeric(colnames(correlations)[colIdx]),
        correlation = correlations[rowIdx, colIdx]
      )
    }
    results <- results %>%
      bind_rows() %>%
      inner_join(outcomes %>%
                   rename(outcomeId1 = "outcomeId",
                          outcomeName1 = "outcomeName"),
                 by = join_by("outcomeId1")) %>%
      inner_join(outcomes %>%
                   rename(outcomeId2 = "outcomeId",
                          outcomeName2 = "outcomeName"),
                 by = join_by("outcomeId2")) %>%
      mutate(
        targetId = as.numeric(gsub("^.*_t", "", gsub("_c.*.csv", "", file))),
        comparatorId = as.numeric(gsub("^.*_c", "", gsub("_a.*.csv", "", file))),
        analyisId = as.numeric(gsub("^.*_a", "", gsub("_[^_]*.csv", "", file))),
        databaseId = gsub("^.*_", "", gsub(".csv", "", file))
      )
    allResults[[length(allResults) + 1]] <- results
  }
  allResults <- allResults %>%
    bind_rows()
  readr::write_csv(allResults, file.path(plotFolder, "HighCorrelations.csv"))
}
