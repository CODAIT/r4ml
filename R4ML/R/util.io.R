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

##logging err # in future may be don't do the stop and just err
hydrar.err <- function(source, message) {
  if (!hydrar.logger$isLoggable("ERROR")) {
    return
  }
  stop("HydraR[" %++% source %++% "]: " %++% message, call. = FALSE)
}

## fatal 
hydrar.fatal <- function(source, message) {
  if (!hydrar.logger$isLoggable("FATAL")) {
    return
  }
  stop("HydraR[" %++% source %++% "]: " %++% message, call. = FALSE)
}
# Prints a warning message
hydrar.warn <- function(source, message, immediate.=FALSE) {
  if (!hydrar.logger$isLoggable("WARN")) {
    return
  }
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
  if (!hydrar.logger$isLoggable("INFO")) {
    return
  }
  if (missing(source)) {
    source <- ""
  }
  if (missing(message)) {
    message <- ""
  }
  infoBit <- trunc(hydrar.env$LOG_LEVEL / 4) %% 2
  if (infoBit == 1) {
    if (length(message) == 1) {
      message("INFO[" %++% source %++% "]: " %++% message)
    } else {
      message("INFO[" %++% source %++% "]: [vector]: " %++% paste(message, collapse=", "))
    }
  }
}


hydrar.infoShow <- function(source, message) {
  if (!hydrar.logger$isLoggable("INFO")) {
    return
  }
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


# debug msg
hydrar.debug <- function(source, message) {
  if (!hydrar.logger$isLoggable("DEBUG")) {
    return
  }
  if (missing(source)) {
    source <- ""
  }
  if (missing(message)) {
    message <- ""
  }
  infoBit <- trunc(hydrar.env$LOG_LEVEL / 4) %% 2
  if (infoBit == 1) {
    if (length(message) == 1) {
      message("DEBUG[" %++% source %++% "]: " %++% message)
    } else {
      message("DEBUG[" %++% source %++% "]: [vector]: " %++% paste(message, collapse=", "))
    }
  }
}

hydrar.fs <- function() {
  logSource <- "hydrar.fs"

  if (SparkR::sparkR.conf()$spark.master == "yarn-client") {
    return("cluster")
  }
  if (SparkR::sparkR.conf()$spark.master == "yarn") {
    return("cluster")
  }
  if (substr(SparkR::sparkR.conf()$spark.master, 1, 8) == "spark://") {
    return("cluster")
  }
  if (substr(SparkR::sparkR.conf()$spark.master, 1, 5) == "local") {
    return("local")
  }
  
  # if the input is not one of the above it is most liklely some other kind of spark cluster
  hydrar.warn(logSource, "Unable to determine file system. Defaulting to cluster mode.")
  return("cluster")
}

hydrar.fs.local <- function() {
  return (ifelse(hydrar.fs()=="local", TRUE, FALSE))
}

hydrar.fs.cluster <- function() {
  return (ifelse(hydrar.fs()=="cluster", TRUE, FALSE))
}

hydrar.hdfs.exist <- function(file) {
  logSource <- "hydrar.hdfs.exist"
  if(hydrar.fs.local()) {
    hydrar.warn(logSource, "Not in cluster mode!") 
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
#' @param na.strings a vector of strings which should be interpreted as NA
#' @param schema (cluster mode) the SparkR scheme
#' @param sep field separator character, supported in local mode
#' @export
hydrar.read.csv <- function(
  file, header = FALSE, stringsAsFactors = FALSE, inferSchema = FALSE, sep = ",",
   na.strings = "NA", schema = NULL, ...
){
  logSource <- "hydrar.read.csv"
  
  if(hydrar.fs.local()) {
    if (!is.null(schema)) {
      hydrar.err(logSource, "Can't have the schema in the local mode")
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

#' HydraR Logging class
#' 
#' @docType class
#' @importFrom R6 R6Class
#' @format A Unified logging utility which control HydraR, SparkR, SystemML log levels
#' @section Methods:
#' \describe{
#'   \item{\code{isValidLevel(logLevel))}}
#'      {check if the log level is valid. Valid log levels are"ALL", "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF"}
#'   \item{\code{setLevel(logLevel, force_java=TRUE))}}
#'      {set the HydraR logger to the specified level. force_java=FALSE will not update the java log level}
#'  \item{\code{getLevel(is_java=FALSE))}}
#'      {get the HydraR loglevel. If is_java is TRUE then return the jvm log level. Usually both of them will be in sync but there is no gurantee}
#'  \item{\code{isLoggable(logLevel))}}
#'      {See if this log level will log the message or not}
#'}      
#' 
#' @examples \dontrun{
#'   mylogger <- HydraR:::Logging$new();
#'   # default levels
#'   level <- mylogger$getLevel();
#'   # change the log level
#'   log$setLevel("ERROR")
#'   mylogger$setLevel(level)
#' }
#' 
Logging <- R6::R6Class(
  "Logging",
  public = list(
    name = "",
    level = "INFO",
    levels = c("ALL", "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF"),
    jlogger = NA,
    initialize = function(name = "HydraR", level = "INFO") {
      self$name <- name
      self$level <- level
      self$jlogger <- get("jlogger", .GlobalEnv)
    },
    isValidLevel = function(val) {
      return (val %in% self$levels)
    },
    setLevel = function(val, force_java=FALSE) {
      if (!self$isValidLevel(val)) {
        msg = paste("invalid levels only ", levels , " supported")
        warning(msg)
      }
      if (force_java) { 
         self$jlogger$setLevel(val)
      }
      self$level <- val
    },
    getLevel = function(is_java=FALSE) {
      if(!is_java) {
        self$level
      } else {
        self$jlogger$getLevel()
      }
    },
    compare = function(level1, level2) {
      if (!self$isValidLevel(level1) || !(self$isValidLevel(level2))) {
         return(NA)
      }
      i1 <- match(level1, self$levels)
      i2 <- match(level2, self$levels)
      return (i1 - i2);
    },
    isLoggable = function(level) {
      cmp <- self$compare(self$level, level)
      if (!is.na(cmp) && cmp <= 0) {
        return(TRUE)
      } else {
        return(FALSE)
      }
    }
))
