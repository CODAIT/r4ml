#
# (C) Copyright IBM Corp. 2015, 2016
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

#' Generate random samples from a hydrar.frame or hydrar.matrix or spark SparkDataFrame
#' 
#' Two sampling methods are supported:
#'
#' 1. Random sampling: Generate a random subset of the given dataset with 
#' the specified size in the form of a percentage.
#' 
#' 2. Partitioned sampling: Split the given dataset into the specified 
#' number of randomly generated non-overlapping subsets.
#' 
#' @name hydrar.sample
#' @title Random sampling
#' @usage hydrar.sample(data, perc)
#' @param data (hydrar.frame or hydrar.matrix or SparkDataFrame) Dataset to sample from
#' @param perc (numeric) For random sampling, an atomic value between (0, 1) 
#'   that represents the sampling percentage. For partitioned sampling, a 
#'   vector of numerics in the interval (0, 1), such that their sum is 
#'   exactly 1.
#' @return For random sampling, a single hydrar.frame/hydrar.matrix/SparkDataFrame is returned. For
#'   partitioned sampling, a list of hydrar.frames or hydrar.matrices or Spark SparkDataFrame is returned, and each
#'   element in the list represents a partition.
#' @export
#' @examples \dontrun{
#'
#' # Generate a 10% random sample of data
#' iris_hf <- as.hydrar.frame(iris)
#' sample_iris_hf <- hydrar.sample(iris_hf, 0.1)
#' 
#' # Generate the 50 samples 
#' sample_50_iris_hf <- hydrar.sample(iris_hf, 50/nrow(iris_hf))
#' 
#' # Randomly split the data into training (70%) and test (30%) sets
#' iris_hf_split <- hydrar.sample(iris_hf, c(0.7, 0.3))
#' nrow(iris_hf_split[[1]]) / nrow(iris_hf)
#' nrow(iris_hf_split[[2]]) / nrow(iris_hf)
#' 
#' # Randomly split the data for 10-fold cross-validation
#' iris_cv <- hydrar.sample(iris_hf, rep(0.1, 10))
#' }

hydrar.sample <- function(data, perc) {
  logSource <- "hydrar.sample"
  
  # Parameter validation
  if (missing(data)) {
    data <- NULL
  }
  if (missing(perc)) {
    perc <- NULL
  }
  
  if (.hydrar.isNullOrEmpty(data)) {
    hydrar.err(logSource, "A dataset must be specified")   
  }    
  if (.hydrar.isNullOrEmpty(perc)) {
    hydrar.err(logSource, "perc perc must be specified.")
  }
  
  if (!inherits(data, "hydrar.frame") & !inherits(data, "hydrar.matrix") & !inherits(data, "SparkDataFrame")) {
    hydrar.err(logSource, "The specified dataset must be a hydrar.frame or hydrar.matrix or Spark SparkDataFrame.")
  }

  # functor to convert the output type to the relevant input type
  outputType = function(...) {
    data_type <- class(data)
    castDF <- function(df) {
      casted_df = df
      if (data_type == 'hydrar.frame') {
        casted_df <- as.hydrar.frame(df)
      } else if (data_type == 'hydrar.matrix') {
        casted_df <- as.hydrar.matrix(as.hydrar.frame(df))
      } else if (data_type == 'SparkDataFrame') {
        casted_df <- df
      } else {
        stop("Unsupported type " %++% data_type %++% " passed in")
      }
      casted_df
    }
    args <- list(...)
    out_type <- sapply(args, castDF)
    out_type
  }
  if (length(perc) == 1) {
    if (perc > 1) {
      hydrar.err(logSource, "perc must be <= 1")
    }
    df1 <- SparkR::sample(data, FALSE, perc)
    return(outputType(df1))
  } else if (length(perc) == 2 && 
      (class(data) %in% c('hydrar.frame', 'hydrar.matrix', 'SparkDataFrame'))) {
    # this is probably slightly faster version of when length(perc)>2
    if (abs(sum(perc) - 1.0) >= 1e-6) {
      hydrar.err(logSource, "Random split must have the value sum to 1")
    } 
    
    perc_1 <- perc[1]
    df1 <- SparkR::sample(data, FALSE, perc_1)
    df2 <- SparkR::except(data, df1)
    out <- c(df1, df2)
    out <- do.call(outputType, list(df1, df2))
    return (out)
  } else if (length(perc) > 2) {
    if (abs(sum(perc) - 1.0) >= 1e-6) {
      stop("error must have the weights perc sum to 1")
    }
    rcolname = "__hydrar_dummy_runif__"
    
    if (rcolname %in% SparkR::colnames(data)) {
      stop("data has already have column " %++% rcolname)
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
      filter_data <- filter_aug_data
      filter_data
    }
    perc_lbound <- cumsum(perc) - perc
    perc_ubound <- cumsum(perc)
    folded_data <- lapply(Map(c, perc_lbound, perc_ubound), create_data_fold)
    out <- do.call(outputType, folded_data)
    return(out)
  } else {
    stop("Other form of sampling not implemented yet")
  }
  return (NULL)
}

