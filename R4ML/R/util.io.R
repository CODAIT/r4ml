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
#
# This file contains io utility functions that can be used all
# accross the project
#
#
read_airline_data <- function() {
  #@TODO deprecate this function
  warning("read_airline_data() has been deprecated. Use data(airline)")
  cat("Reading airline data set")
  fpath <- system.file("extdata", "airline.zip", package="HydraR")
  airt <- read.table(unz(fpath, "airline.csv"), sep=",", header=TRUE)

}

##logging info
hydrar.err <- function(source, message) {
  stop("HydraR[" %++% source %++% "]: " %++% message)
}

# Prints a warning message
hydrar.warn <- function(source, message, immediate.=FALSE) {
  if (missing(source)) {
    source <- ""
  }
  if (missing(message)) {
    message <- ""
  }
  warnBit <- trunc(hydrar.env$LOG_LEVEL / 2) %% 2
  if (warnBit == 1) {
    warning("[" %++% source %++% "]: " %++% message, call. = FALSE, immediate.=immediate.)
  }
}

# Prints an information message
hydrar.info <- function(source, message) {
  if (missing(source)) {
    source <- ""
  }
  if (missing(message)) {
    message <- ""
  }
  infoBit <- trunc(hydrar.env$LOG_LEVEL / 4) %% 2
  if (infoBit == 1) {
    if (length(message) == 1) {
      cat("DEBUG[" %++% source %++% "]: " %++% message)
      cat("\n")
    } else {
      cat("DEBUG[" %++% source %++% "]: [vector]: " %++% paste(message, collapse=", "))
      cat("\n")
    }
  }
}

hydrar.infoShow <- function(source, message) {
  if (missing(source)) {
    source <- ""
  }
  if (missing(message)) {
    message <- ""
  }
  infoBit <- trunc(hydrar.env$LOG_LEVEL / 4) %% 2
  if (infoBit == 1) {
    cat("INFO[" %++% source %++% "]: ")
    show(message)
    cat("\n")
  }
}

hydrar.fs <- function() {
  hydrar_client <- hydrar.env$HYDRAR_CLIENT()
  if (hydrar_client == "yarn-client") {
    return("cluster")
  }
  if (hydrar_client == "yarn") {
    return("cluster")
  }
  if (substr(hydrar_client, 1, 8) == "spark://") {
    return("cluster")
  }
  if (substr(hydrar_client, 1, 5) == "local") {
    return("local")
  }
  
  # if the input is not one of the above it is most liklely some other kind of spark cluster
  warning("Unable to determine file system. Assuming cluster mode.")
  return("cluster")
}

hydrar.fs.local <- function() {
  return (ifelse(hydrar.fs()=="local", TRUE, FALSE))
}

hydrar.fs.cluster <- function() {
  return (ifelse(hydrar.fs()=="cluster", TRUE, FALSE))
}

hydrar.hdfs.exist <- function(file) {
  if(hydrar.fs.local()) {
    warning("Not in cluster mode!") 
    return(FALSE)
  }
  
  # hdfs commands can take a few seconds to return a result. try to avoid calling this function if possible
  exists <- (length(as.character(system(paste0("(hdfs dfs -test -e \"", file, "\") || echo \"fail\""), intern=TRUE)))==0)
  
  return(exists)
}

#' hydrar.read.csv
#' @description Returns a R data.frame in local mode or a SparkR DataFrame in cluster mode.
#' @param file path to the input file
#' @param header logical
#' @param stringsAsFactors logical
#' @param inferSchema logical
#' @param sep field separator character, supported in local mode
#' @export
hydrar.read.csv <- function(
  file, header = FALSE, stringsAsFactors = FALSE, inferSchema = FALSE, sep = ",",
   na.strings = "NA", schema = NULL, ...
){
  if(hydrar.fs.local()) {
    if (!is.null(schema)) {
      hydrar.err("Can't have the schema in the local mode")
    }
    df <- utils::read.csv(file, header = header, stringsAsFactors = stringsAsFactors, sep = sep, na.strings = na.strings)
    return(df)
  }
  
  # we need to pass in these arguments as strings
  header_val <- ifelse(header, "true", "false")
  stringsAsFactors_val <- ifelse(stringsAsFactors, "true", "false")
  inferSchema_val <- ifelse(inferSchema, "true", "false")
  
  # old way of reading using the external pacakge com.databricks.spark.csv
  # df <- SparkR::read.df(sysmlSqlContext,
  #                       file,
  #                       source = "com.databricks.spark.csv",
  #                       header = header_val,
  #                       stringsAsFactors = stringsAsFactors_val,
  #                       inferSchema = inferSchema_val)
  source = "csv" # we always default to csv user can use read.df for other things
  df <- SparkR::read.df(
          file,
          source = source,
          header = header_val,
          inferSchema = inferSchema_val,
          na.strings = na.strings
        )
  return(df)
}
