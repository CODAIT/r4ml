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

r4ml_ascii <- function() {
  
  # Big by Glenn Chappell 4/93 -- based on Standard
  # Includes ISO Latin-1
  # Greek characters by Bruce Jakeway <pbjakeway@neumann.uwaterloo.ca>
  # figlet release 2.2 -- November 1996
  # Permission is hereby given to modify this font, as long as the
  # modifier's name is placed on a comment line.
  #
  # Modified by Paul Burton  12/96 to include new parameter
  # supported by FIGlet and FIGWin.  May also be slightly modified for better use
  # of new full-width/kern/smush alternatives, but default output is NOT changed.
  
  ascii <- "\n"
  ascii <- paste0(ascii, "  _______   _    _    ____    ____  _____\n")
  ascii <- paste0(ascii, " |_   __ \\ | |  | |  |_   \\  /   _||_   _|    \n")
  ascii <- paste0(ascii, "   | |__) || |__| |_   |   \\/   |    | |      \n")
  ascii <- paste0(ascii, "   |  __ / |____   _|  | |\\  /| |    | |   _  \n")
  ascii <- paste0(ascii, "  _| |  \\ \\_   _| |_  _| |_\\/_| |_  _| |__/ | \n")
  ascii <- paste0(ascii, " |____| |___| |_____||_____||_____||________| \n")
  ascii <- paste0(ascii, " \n")
  return(ascii)
}

systemml_ascii <- function() {
  
  # Big by Glenn Chappell 4/93 -- based on Standard
  # Includes ISO Latin-1
  # Greek characters by Bruce Jakeway <pbjakeway@neumann.uwaterloo.ca>
  # figlet release 2.2 -- November 1996
  # Permission is hereby given to modify this font, as long as the
  # modifier's name is placed on a comment line.
  #
  # Modified by Paul Burton  12/96 to include new parameter
  # supported by FIGlet and FIGWin.  May also be slightly modified for better use
  # of new full-width/kern/smush alternatives, but default output is NOT changed.

  ascii <- "\n"
  ascii <- paste0(ascii, "   _____           _                 __  __ _\n")
  ascii <- paste0(ascii, "  / ____|         | |               |  \\/  | |\n")
  ascii <- paste0(ascii, " | (___  _   _ ___| |_ ___ _ __ ___ | \\  / | |\n")
  ascii <- paste0(ascii, "  \\___ \\| | | / __| __/ _ \\ '_ ` _ \\| |\\/| | |\n")
  ascii <- paste0(ascii, "  ____) | |_| \\__ \\ ||  __/ | | | | | |  | | |____\n")
  ascii <- paste0(ascii, " |_____/ \\__, |___/\\__\\___|_| |_| |_|_|  |_|______|\n")
  ascii <- paste0(ascii, "          __/ |\n")
  ascii <- paste0(ascii, "         |___/\n")
  return(ascii)
}

# this will be called at the library loading time.
# it first check if the sparkR packages is loaded?
#   (which one must before using R4ML)
# the it initialize SparkR basically starting the cluster and setting the
# internals of sparkR with extra loaded jar from systemML so that one
# call the systemML as well as SparkR functionality as needed
# if sparkR is already initialized then it wil re-initialize it
.onLoad <- function(libname, pkgname) {
  logsrc <- ".onLoad"

  if (Sys.getenv("R4ML_AUTO_START") == "1") {
    r4ml.info(logsrc, "Auto starting session")
    r4ml.session()
  }
}

.onAttach <- function(libname, pkgname) {
  packageStartupMessage(r4ml_ascii())

  # print the version
  if (nchar(libname) > 0 & nchar(pkgname) > 0) { 
    desc <- utils::packageDescription(pkg = pkgname, lib.loc = libname)
    packageStartupMessage("version ", desc$Version)
  }
}

# This function is called at the unloading of the R4ML library or at the time
# of detaching it.
# it basically stops the SparkR cluster and unload any namespace associated with it
.onUnload <- function(libpath) {
  if (r4ml.env$R4ML_SESSION_EXISTS) {
    SparkR::sparkR.session.stop()
  }
}

.onDetach <- function(libpath) {
  .onUnload(libpath)
}

#' Initialize an R4ML Session
#' 
#' r4ml.session is a wrapper function for sparkR.session
#' @param master (character):  Spark cluster mode (typically either local[*] or yarn).
#' @param sparkHome (character):  Path to SPARK_HOME 
#' @param sparkConfig (list): Configuration options to be passed to sparkR.session()
#' @param ... other arguments to be passed to sparkR.session()
#' @export
r4ml.session <- function(
  sparkHome = Sys.getenv("SPARK_HOME"),
  sparkConfig = list("spark.driver.memory" = Sys.getenv("R4ML_SPARK_DRIVER_MEMORY")),
  ...
  ) {
  logsrc <- "r4ml.session"

  if (r4ml.env$R4ML_SESSION_EXISTS) {
    r4ml.warn(logsrc, "R4ML session already initialized")
    return(invisible(NULL))
  }

  if (nchar(sparkHome) == 0) {
    r4ml.err(logsrc, "SPARK_HOME not defined")
  }

  if (length(sparkConfig$spark.driver.memory) == 0  || nchar(sparkConfig$spark.driver.memory) == 0) {
    r4ml.warn(logsrc, " driver.memory not defined. Defaulting to 2G")
    sparkConfig$spark.driver.memory <- "2G"
  }

  # SparkR session init
  r4ml.env$sc <- SparkR::sparkR.session(
    appName = "R4ML",
    sparkHome = sparkHome,
    sparkConfig = sparkConfig,
    sparkJars = r4ml.env$SYSML_JARS(),
    sparkPackages = "",
    ...)

  # we get the sparkContext as in the systemML most of the code uses the sparkContext
  r4ml.env$sysmlSparkContext <- SparkR::sparkR.callJMethod(r4ml.env$sc, "sparkContext")

  # we will use now the Java Spark Context
  r4ml.env$sysmlJavaSparkContext <- SparkR::sparkR.newJObject("org.apache.spark.api.java.JavaSparkContext", r4ml.env$sysmlSparkContext)

  # since sysmlSqlContext is used for most of the systemML related
  # code we need to have it.
  r4ml.env$sysmlSqlContext <- SparkR::sparkR.callJMethod(r4ml.env$sc, "sqlContext")
  
  # java logger and level settings
  r4ml.env$jlogger <- log4j.Logger$new("org")
  r4ml.env$jlogger$setLevel("WARN")

  # static class sysml.RDDUtils
  # since actual class is static, we need this
  r4ml.env$sysml.RDDUtils <- sysml.RDDConverterUtils$new()

  # R4ML logger
  logger <- Logging$new()
  r4ml.env$r4ml.logger <- logger
  loglevel <- r4ml.env$DEFAULT_LOG_LEVEL
  env_loglevel <- Sys.getenv("R4ML_LOG_LEVEL")
  if (nchar(env_loglevel) != 0) {
    loglevel <- base::toupper(env_loglevel)
    r4ml.info(logsrc, sprintf("Changing the log level to %s as per env 'R4ML_LOG_LEVEL'", loglevel))
  }
  logger$setLevel(loglevel)

  # create the r4ml.fs
  fs <- create.r4ml.fs()
  r4ml.env$r4ml.fs <- fs

  # post startup message
  loglevel <- r4ml.env$DEFAULT_LOG_LEVEL
  packageStartupMessage(sprintf("[%s]: Default log level will be set to '%s'", logsrc, loglevel))
  packageStartupMessage(sprintf("[%s]: To change call r4ml.setLogLevel(NewLevel)", logsrc))

  # mark the session exists flag
  r4ml.env$R4ML_SESSION_EXISTS <- TRUE
}

#' Session Stop
#' 
#' Stops an existing R4ML session
#' @export
r4ml.session.stop <- function() {
  if ("r4ml.logger" %in% ls(r4ml.env)) { rm("r4ml.logger", envir = r4ml.env) }
  if ("r4ml.fs" %in% ls(r4ml.env)) { rm("r4ml.fs", envir = r4ml.env) }
  if ("jlogger" %in% ls(r4ml.env)) { rm("jlogger", envir = r4ml.env) }
  if ("sysml.RDDUtils" %in% ls(r4ml.env)) { rm("sysml.RDDUtils", envir = r4ml.env) }
  if ("sysmlSparkContext" %in% ls(r4ml.env)) { rm("sysmlSparkContext", envir = r4ml.env) }
  if ("sysmlSqlContext" %in% ls(r4ml.env)) { rm("sysmlSqlContext", envir = r4ml.env) }
  if ("sc" %in% ls(r4ml.env)) { rm("sc", envir = r4ml.env) }

  SparkR::sparkR.session.stop()
  r4ml.env$R4ML_SESSION_EXISTS <- FALSE
}
