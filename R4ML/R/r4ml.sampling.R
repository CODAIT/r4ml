#
# (C) Copyright IBM Corp. 2017
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
#
#' @include zzz.R
NULL

## note: implement all the sample algo later

#' Generate random samples from a r4ml.frame or r4ml.matrix or spark SparkDataFrame
#' 
#' Two sampling methods are supported:
#'
#' 1. Random sampling: Generate a random subset of the given dataset with 
#' the specified size in the form of a percentage.
#' 
#' 2. Partitioned sampling: Split the given dataset into the specified 
#' number of randomly generated non-overlapping subsets.
#' 
#' @name r4ml.sample
#' @title Random sampling
#' @param data (r4ml.frame or r4ml.matrix or SparkDataFrame) Dataset to sample from
#' @param perc (numeric) For random sampling, an atomic value between (0, 1) 
#'   that represents the sampling percentage. For partitioned sampling, a 
#'   vector of numerics in the interval (0, 1), such that their sum is 
#'   exactly 1.
#' @param experimental (logical)
#' @param cache (logical) if TRUE the output is cached
#' @return For random sampling, a single r4ml.frame/r4ml.matrix/SparkDataFrame is returned. For
#'   partitioned sampling, a list of r4ml.frames or r4ml.matrices or Spark SparkDataFrame is returned, and each
#'   element in the list represents a partition.
#' @export
#' @examples \dontrun{
#'
#' # Generate a 10% random sample of data
#' iris_hf <- as.r4ml.frame(iris)
#' sample_iris_hf <- r4ml.sample(iris_hf, 0.1)
#' 
#' # Generate the 50 samples 
#' sample_50_iris_hf <- r4ml.sample(iris_hf, 50/nrow(iris_hf))
#' 
#' # Randomly split the data into training (70%) and test (30%) sets
#' iris_hf_split <- r4ml.sample(iris_hf, c(0.7, 0.3))
#' nrow(iris_hf_split[[1]]) / nrow(iris_hf)
#' nrow(iris_hf_split[[2]]) / nrow(iris_hf)
#' 
#' # Randomly split the data for 10-fold cross-validation
#' iris_cv <- r4ml.sample(iris_hf, rep(0.1, 10))
#' }

r4ml.sample <- function(data, perc, experimental=FALSE,
                          cache = FALSE) {
  logSource <- "r4ml.sample"
  
  # Parameter validation
  if (missing(data)) {
    data <- NULL
  }
  if (missing(perc)) {
    perc <- NULL
  }
  
  if (.r4ml.isNullOrEmpty(data)) {
    r4ml.err(logSource, "A dataset must be specified")   
  }    
  if (.r4ml.isNullOrEmpty(perc)) {
    r4ml.err(logSource, "perc must be specified.")
  }
  
  if (!inherits(data, "r4ml.frame") & !inherits(data, "r4ml.matrix") & !inherits(data, "SparkDataFrame")) {
    r4ml.err(logSource, "The specified dataset must either be a r4ml.frame, r4ml.matrix, or SparkDataFrame.")
  }

  # function to convert the output type to the relevant input type
  outputType = function(...) {
    data_type <- class(data)
    castDF <- function(df) {
      casted_df = df
      if (data_type == 'r4ml.frame') {
        casted_df <- as.r4ml.frame(df, repartition = FALSE)
      } else if (data_type == 'r4ml.matrix') {
        casted_df <- as.r4ml.matrix(df)
      } else if (data_type == 'SparkDataFrame') {
        casted_df <- df
      } else {
        r4ml.err(logSource, "Unsupported type " %++% data_type %++% " passed in")
      }
     
      if (cache & !casted_df@env$isCached) {
        dummy <- SparkR::cache(casted_df)
      }
      
      casted_df
    }
    args <- list(...)
    out_type <- sapply(args, castDF)
    out_type
  }
  if (length(perc) == 1) {
    if (perc > 1) {
      r4ml.err(logSource, "perc must be <= 1")
    }
    df1 <- SparkR::sample(data, FALSE, perc)
    return(outputType(df1))
  } else if (length(perc) == 2 && experimental == TRUE && # this features doesn't always work on cluster
      (class(data) %in% c('r4ml.frame', 'r4ml.matrix', 'SparkDataFrame'))) {
    # this is probably slightly faster version of when length(perc)>2
    if (abs(sum(perc) - 1.0) >= 1e-6) {
      r4ml.err(logSource, "Random splits must sum to 1")
    } 
    
    perc_1 <- perc[1]
    df1 <- SparkR::sample(data, FALSE, perc_1)
    df2 <- SparkR::except(data, df1)
    out <- c(df1, df2)
    out <- do.call(outputType, list(df1, df2))

    return (out)
  } else if (length(perc) >= 2) {
    if (abs(sum(perc) - 1.0) >= 1e-6) {
      r4ml.err(logSource, "sum of perc weights must be equal 1")
    }
    rcolname = "__r4ml_dummy_runif__"
    
    if (rcolname %in% SparkR::colnames(data)) {
      r4ml.err(logSource, "data already has column " %++% rcolname)
    }
    # create the uniform random (0,1) in the column rcolname
    aug_data <- SparkR::withColumn(data, rcolname, SparkR::rand())
    aug_data_cnames <- SparkR::colnames(aug_data)

    # partition the column based the uniform disribution
    create_data_fold <- function (bounds) {
      lb <- bounds[[1]]
      ub <- bounds[[2]]
      predicate <- lb %++% " <= " %++% rcolname %++% " and " %++% rcolname %++% " < " %++% ub
      filter_aug_data <- SparkR::filter(aug_data, predicate)
      cnames <- SparkR::colnames(aug_data)
      filter_data <- SparkR::select(filter_aug_data, 
                                    aug_data_cnames[aug_data_cnames != rcolname])
      #filter_data <- filter_aug_data
      filter_data
    }
    perc_lbound <- cumsum(perc) - perc
    perc_ubound <- cumsum(perc)
    folded_data <- lapply(Map(c, perc_lbound, perc_ubound), create_data_fold)
    out <- do.call(outputType, folded_data)
    return(out)
  } else {
    r4ml.err(logSource, "Other forms of sampling not implemented yet")
    #@TODO
  }
  return (NULL)
}

