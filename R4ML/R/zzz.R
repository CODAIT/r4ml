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

auto_start_session <- function() {
  logSource <- "auto_start_session"
  
  if (Sys.getenv("R4ML_AUTO_START") == "1") {
    if (nchar(Sys.getenv("SPARK_HOME")) == 0){
      r4ml.warn(logSource, "unable to start session. 'SPARK_HOME' not defined")
      return(FALSE)
      }
    return(TRUE)
    }
  
  return(FALSE)
}

# this will be called at the library loading time.
# it first check if the sparkR packages is loaded?
#   (which one must before using R4ML)
# the it initialize SparkR basically starting the cluster and setting the
# internals of sparkR with extra loaded jar from systemML so that one
# call the systemML as well as SparkR functionality as needed
# if sparkR is already initialized then it wil re-initialize it
.onLoad <- function(libname, pkgname) {
  logsrc <- r4ml.env$PACKAGE_NAME

  if ("SparkR" %in% loadedNamespaces()) {
    if (exists(".sparkRjsc", envir = SparkR:::.sparkREnv)) {
      r4ml.warn(logsrc, "Reloading SparkR")
      r4ml.reload.SparkR()
    } else {
      r4ml.info(logsrc, "Loading SparkR")
      r4ml.load.SparkR()
    }
  }

  r4ml.startup.message(libname, pkgname)

  if(auto_start_session()) {
    r4ml.info(logsrc, "Auto starting session")
    r4ml.session()
  }
}

r4ml.startup.message <- function(libname, pkgname, ...) {
  logsrc <- r4ml.env$PACKAGE_NAME
  # print the logo
  packageStartupMessage(r4ml_ascii())

  # print the version
  if(nchar(libname) > 0 & nchar(pkgname) > 0) { 
    desc <- utils::packageDescription(pkg = pkgname, lib.loc = libname)
    vstr <- sprintf("[%s]: version %s", logsrc, desc$Version)
    packageStartupMessage(vstr)
  }
}

r4ml.post.startup.message <- function(...) {
  logsrc <- r4ml.env$PACKAGE_NAME
    # print the other important info
  loglevel <- r4ml.env$DEFAULT_LOG_LEVEL
  packageStartupMessage(
    sprintf("[%s]: Default log level will be set to '%s'", logsrc, loglevel))
  packageStartupMessage(
    sprintf("[%s]: To change call r4ml.setLogLevel(NewLevel)", logsrc))
}

# a utlity function to check is sparkR loaded and is initialized
is.sparkr.ready <- function() {
  if ("SparkR" %in% loadedNamespaces()) {
    if (exists(".sparkRjsc", envir = SparkR:::.sparkREnv)) {
      return (TRUE)
    }
    return (FALSE)
  }
}

# This function is called at the unloading of the R4ML library or at the time
# of detaching it.
# it basically stops the SparkR cluster and unload any namespace associated with it
#
.onUnload <- function(libpath) {
  if (r4ml.env$R4ML_SESSION_EXISTS) {
    r4ml.session.stop()
  }
  
  r4ml.unload.SparkR()
}

.onDetach <- function(libpath) {
  .onUnload()
}

# a utlity function to check is sparkR loaded
is.sparkr.loaded <- function() {
  if ("SparkR" %in% loadedNamespaces()) {
    return (TRUE)
  } else {
    return (FALSE)
  }
}

# first stop the SparkR cluster and detach it
# then restart SparkR with systemML and attach it to env
r4ml.reload.SparkR <- function() {
  r4ml.unload.SparkR()
  r4ml.load.SparkR()
}


#load and initialize SparkR
r4ml.load.SparkR <- function() {
  logsrc <- r4ml.env$PACKAGE_NAME
  if (!requireNamespace("SparkR")) {
    r4ml.warn(logSource, "SparkR not found in the standard library or in the R_LIBS path")

    lib_loc <- file.path(Sys.getenv("SPARK_HOME"), "R", "lib")
    .libPaths(c(lib_loc, .libPaths()))
    #library(SparkR, lib.loc = c(lib_loc))
  }
  
  # force the sparkR pacakge requirement
  requireNamespace("SparkR")
}

# unload SparkR and claim the cluster and cleanup
r4ml.unload.SparkR <- function() {
  SparkR::sparkR.stop()
  detach_package("SparkR", character.only = TRUE)
}

#' detach the package recursively (even if multiple version exists)
#'
#' It is the wrapper over the base detach package
#'
#' @examples \dontrun{
#' library("survival")
#' detach_package("survival")
#' }
#' @export
detach_package <- function(pkg, character.only = FALSE) {
  if(!character.only) {
    pkg <- deparse(substitute(pkg))
  }
  search_item <- paste("package", pkg, sep = ":")
  while(search_item %in% search()) {
    detach(search_item, unload = TRUE, character.only = TRUE)
  }
}

#systemML related initiation
sysml.init <- function() {
  #@TODO eventually all the variables should be moved to r4ml.env
  
  # spark context is now replace by sparkSession
  sc <- get("sc", envir = .GlobalEnv)
  
  # we get the sparkContext as in the systemML most of the code uses the sparkContext
  sysmlSparkContext <- SparkR:::callJMethod(sc, "sparkContext")
  assign("sysmlSparkContext", sysmlSparkContext, .GlobalEnv)

  # we will use now the Java Spark Context
  sysmlJavaSparkContext <- SparkR:::newJObject("org.apache.spark.api.java.JavaSparkContext",
                        sysmlSparkContext)
  assign("sysmlJavaSparkContext", sysmlJavaSparkContext, .GlobalEnv)
  
  # since sysmlSqlContext is used for most of the systemML related
  # code we need to have it.
  sysmlSqlContext <- SparkR:::callJMethod(sc, "sqlContext")
  assign("sysmlSqlContext", sysmlSqlContext, .GlobalEnv)
  
  # java logger and level settings
  jlogger <- log4j.Logger$new("org")
  assign("jlogger", jlogger, .GlobalEnv)
  jlogger$setLevel("WARN")

  # static class sysml.RDDUtils
  # since actual class is static, we need this
  sysml.RDDUtils <- sysml.RDDConverterUtils$new()
  assign("sysml.RDDUtils", sysml.RDDUtils, .GlobalEnv)
}

# systemML related stopping i.e de-initialization
sysml.stop <- function() {
  if("jlogger" %in% ls(.GlobalEnv)) { rm(jlogger, envir = .GlobalEnv) }
  if("sysml.RDDUtils" %in% ls(.GlobalEnv)) { rm(sysml.RDDUtils, envir = .GlobalEnv) }
  if("sysmlSparkContext" %in% ls(.GlobalEnv)) { rm(sysmlSparkContext, envir = .GlobalEnv) }
  if("sysmlJavaSparkContext" %in% ls(.GlobalEnv)) { rm(sysmlJavaSparkContext, envir = .GlobalEnv) }
  if("sysmlSqlContext" %in% ls(.GlobalEnv)) { rm(sysmlSqlContext, envir = .GlobalEnv) }
}

# R4ML related initialization
r4ml.init <- function() {
  logsrc <- r4ml.env$PACKAGE_NAME
  # R4ML logger
  logger <- Logging$new()
  assign("r4ml.logger", logger, .GlobalEnv)
  loglevel <- r4ml.env$DEFAULT_LOG_LEVEL
  env_loglevel <- Sys.getenv("R4ML_LOG_LEVEL")
  if (nchar(env_loglevel) != 0) {
    loglevel <- base::toupper(env_loglevel)
    r4ml.info(logsrc, 
      sprintf("Changing the log level to %s as per env 'R4ML_LOG_LEVEL'", loglevel))
  }
  logger$setLevel(loglevel)
  
  # create the r4ml.fs
  fs <- create.r4ml.fs()
  assign("r4ml.fs", fs, .GlobalEnv)
  
  # assign the current sparkr.env to the r4ml.env
  assign("sparkr.env", SparkR:::.sparkREnv, r4ml.env)

  # post startup message
  r4ml.post.startup.message()

  # mark the session exists flag
  r4ml.env$R4ML_SESSION_EXISTS <- TRUE
  
}

r4ml.end <- function() {
  if("r4ml.logger" %in% ls(.GlobalEnv)) { rm(r4ml.logger, envir = .GlobalEnv) }
  
  if("r4ml.fs" %in% ls(.GlobalEnv)) { rm(r4ml.fs, envir = .GlobalEnv) }
  
  r4ml.env$R4ML_SESSION_EXISTS <- FALSE
}

#' Initialize a R4ML Session
#' 
#' r4ml.session is a wrapper function for sparkR.session
#' @param master typically either local[*] or yarn
#' @param sparkHome path to spark
#' @param sparkConfig configuration options to be passed to sparkR.session()
#' @param ... other arguments to be passed to sparkR.session()
#' @export
r4ml.session <- function(
  sparkHome = Sys.getenv("SPARK_HOME"),
  sparkConfig = list("spark.driver.memory" = Sys.getenv("R4ML_SPARK_DRIVER_MEMORY")),
  ...
  ) {
  logsrc <- r4ml.env$PACKAGE_NAME
  #@TODO in the future make the signature of this function match sparkr.session()
  
  if (r4ml.env$R4ML_SESSION_EXISTS) {
    r4ml.warn(logsrc, " R4ML session already initialized")
    return()
  }
  
  if (nchar(sparkHome) == 0) {
    r4ml.err(logsrc, "SPARK_HOME not defined")
  }
  
  if (length(sparkConfig$spark.driver.memory) == 0  || nchar(sparkConfig$spark.driver.memory) == 0) {
    r4ml.warn(logsrc, " driver.memory not defined. Defaulting to 2G")
    sparkConfig$spark.driver.memory <- "2G"
  }
  
  # SparkR session init
  sparkr.init <- function(...) {
    sc <- SparkR::sparkR.session(
      appName = "R4ML",
      sparkHome = sparkHome,
      sparkConfig = sparkConfig,
      sparkJars = r4ml.env$SYSML_JARS(),
      sparkPackages = "",
      ...)
    # spark context is now replace by sparkSession
    assign("sc", sc, envir = .GlobalEnv)
  }
  sparkr.init(...)
  
  #sysml related initialization
  sysml.init()
  
  #r4ml related initialization
  r4ml.init()
}

#' Session Stop
#' 
#' Stops an existing R4ML session
#' @export
r4ml.session.stop <- function() {
  logsrc <- r4ml.env$PACKAGE_NAME
  
  # this should be in the reverse order as the init to avoid any potential variable 
  # dependency issues in future addition of variables and func
  
  r4ml.end()
  
  sysml.stop()
  
  # clean sparkR
  sparkr.stop <- function() {
     if("sc" %in% ls(.GlobalEnv)) { rm(sc, envir = .GlobalEnv) }
     # only remove if r4ml started the sparkr session
     if (identical(SparkR:::.sparkREnv, r4ml.env$sparkr.env)) { 
       if (exists(".sparkRjsc", envir = SparkR:::.sparkREnv)) {
         SparkR::sparkR.session.stop()
       }
     }
  }
  sparkr.stop()
 
}
