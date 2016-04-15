#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
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
  cat("Reading airline data set")
  fpath <- system.file("extdata", "airline.zip", package="iSparkR")
  airt <- read.table(unz(fpath, "airline.csv"), sep=",", header=TRUE)

}

##logging info
hydrar.err <- function(source, message) {
  stop("HydraR[" %++% source %++% "]: " %++% message)
}

# Prints a warning message
hydrar.warn <- function(source, message, immediate.=F) {
  if (missing(source)) {
    source <- ""
  }
  if (missing(message)) {
    message <- ""
  }
  warnBit <- trunc(hydrar.env$LOG_LEVEL / 2) %% 2
  if (warnBit == 1) {
    warning("[" %++% source %++% "]: " %++% message, call. = F, immediate.=immediate.)
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
