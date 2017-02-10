#
# (C) Copyright IBM Corp. 2015-2017
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

hydrar_ascii <- function() {
  
  #' Big by Glenn Chappell 4/93 -- based on Standard
  #' Includes ISO Latin-1
  #' Greek characters by Bruce Jakeway <pbjakeway@neumann.uwaterloo.ca>
  #' figlet release 2.2 -- November 1996
  #' Permission is hereby given to modify this font, as long as the
  #' modifier's name is placed on a comment line.
  #'
  #' Modified by Paul Burton  12/96 to include new parameter
  #' supported by FIGlet and FIGWin.  May also be slightly modified for better use
  #' of new full-width/kern/smush alternatives, but default output is NOT changed.
  
  ascii <- "\n"
  ascii <- paste0(ascii, "  _    _           _           _____\n")
  ascii <- paste0(ascii, " | |  | |         | |         |  __ \\\n") 
  ascii <- paste0(ascii, " | |__| |_   _  __| |_ __ __ _| |__) |\n")
  ascii <- paste0(ascii, " |  __  | | | |/ _` | '__/ _` |  _  /\n")
  ascii <- paste0(ascii, " | |  | | |_| | (_| | | | (_| | | \\ \\\n")
  ascii <- paste0(ascii, " |_|  |_|\\__, |\\__,_|_|  \\__,_|_|  \\_\\\n")
  ascii <- paste0(ascii, "          __/ |\n")
  ascii <- paste0(ascii, "         |___/\n")
  return(ascii)
}

systemml_ascii <- function() {
  
  #' Big by Glenn Chappell 4/93 -- based on Standard
  #' Includes ISO Latin-1
  #' Greek characters by Bruce Jakeway <pbjakeway@neumann.uwaterloo.ca>
  #' figlet release 2.2 -- November 1996
  #' Permission is hereby given to modify this font, as long as the
  #' modifier's name is placed on a comment line.
  #'
  #' Modified by Paul Burton  12/96 to include new parameter
  #' supported by FIGlet and FIGWin.  May also be slightly modified for better use
  #' of new full-width/kern/smush alternatives, but default output is NOT changed.

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
  
  if (Sys.getenv("HYDRAR_AUTO_START") == "1") {
    if (nchar(Sys.getenv("SPARK_HOME")) == 0){
      hydrar.warn(logSource, "unable to start session. 'SPARK_HOME' not defined")
      return(FALSE)
      }
    return(TRUE)
    }
  
  return(FALSE)
}

# this will be called at the library loading time.
# it first check if the sparkR packages is loaded?
#   (which one must before using HydraR)
# the it initialize SparkR basically starting the cluster and setting the
# internals of sparkR with extra loaded jar from systemML so that one
# call the systemML as well as SparkR functionality as needed
# if sparkR is already initialized then it wil re-initialize it
.onLoad <- function(libname, pkgname) {
  logSource <- ".onLoad"
  
  if (Sys.getenv("HYDRAR_DEBUG_MODE") == "1") {
    hydrar.env$LOG_LEVEL <-4
    hydrar.info(logSource, "debug mode activated")
  }

  if ("SparkR" %in% loadedNamespaces()) {
    if (exists(".sparkRjsc", envir = SparkR:::.sparkREnv)) {
      hydrar.warn(logSource, "Reloading SparkR")
      hydrar.reload.SparkR()
    } else {
      hydrar.info(logSource, "loading SparkR")
      hydrar.load.SparkR()
    }
  }

  packageStartupMessage(hydrar_ascii())

  if(nchar(libname) > 0 & nchar(pkgname) > 0) { 
    # in some environments libname and pkgname may not be passed
    desc <- utils::packageDescription(pkg = pkgname, lib.loc = libname)
    packageStartupMessage(paste("version", desc$Version))
  }
  
  if(auto_start_session()) {
    hydrar.info(logSource, "auto starting session")
    hydrar.session()
  }
  
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

# This function is called at the unloading of the HydraR library or at the time
# of detaching it.
# it basically stops the SparkR cluster and unload any namespace associated with it
#
.onUnload <- function(libpath) {
  if (hydrar.env$HYDRAR_SESSION_EXISTS) {
    hydrar.session.stop()
  }
  
  hydrar.unload.SparkR()
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
hydrar.reload.SparkR <- function() {
  hydrar.unload.SparkR()
  hydrar.load.SparkR()
}


#load and initialize SparkR
hydrar.load.SparkR <- function() {
  logSource <- "hydrar.load.SparkR"
  if (!requireNamespace("SparkR")) {
    hydrar.warn(logSource, "SparkR not found in the standard library or in the R_LIBS path")

    lib_loc <- file.path(Sys.getenv("SPARK_HOME"), "R", "lib")
    .libPaths(c(lib_loc, .libPaths()))
    #library(SparkR, lib.loc = c(lib_loc))
  }
  
  requireNamespace("SparkR")

}

# unload SparkR and claim the cluster and cleanup
hydrar.unload.SparkR <- function() {
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

sysml.init <- function(sc) {
  
  #@TODO eventually all the variables should be moved to hydrar.env
  
  # spark context is now replace by sparkSession
  assign("sc", sc, envir = .GlobalEnv)
  
  # we get the sparkContext as in the systemML most of the code uses the sparkContext
  sysmlSparkContext <- SparkR:::callJMethod(sc, "sparkContext")
  assign("sysmlSparkContext", sysmlSparkContext, .GlobalEnv)

  # since sysmlSqlContext is used for most of the systemML related
  # code we need to have it.
  sysmlSqlContext <- SparkR:::callJMethod(sc, "sqlContext")
  assign("sysmlSqlContext", sysmlSqlContext, .GlobalEnv)
  
  # logger and level settings
  logger <- log4j.Logger$new("org")
  assign("logger", logger, .GlobalEnv)
  logger$setLevel("WARN")

  # static class sysml.RDDUtils
  # since actual class is static, we need this
  sysml.RDDUtils <- sysml.RDDConverterUtils$new()
  assign("sysml.RDDUtils", sysml.RDDUtils, .GlobalEnv)
}

sysml.stop <- function() {
  
  if("logger" %in% ls(.GlobalEnv)) { rm(logger, envir = .GlobalEnv) }
  if("sc" %in% ls(.GlobalEnv)) { rm(sc, envir = .GlobalEnv) }
  if("sysml.RDDUtils" %in% ls(.GlobalEnv)) { rm(sysml.RDDUtils, envir = .GlobalEnv) }
  if("sysmlSparkContext" %in% ls(.GlobalEnv)) { rm(sysmlSparkContext, envir = .GlobalEnv) }
  if("sysmlSqlContext" %in% ls(.GlobalEnv)) { rm(sysmlSqlContext, envir = .GlobalEnv) }
  
}

#' hydrar.session
#' @description Initialize HydraR
#' hydrar.session is a wrapper function for sparkR.session
#' @param master typically either local[*] or yarn
#' @param sparkHome path to spark
#' @param sparkConfig configuration options to be passed to sparkR.session()
#' @param ... other arguments to be passed to sparkR.session()
#' @export
hydrar.session <- function(
  master = Sys.getenv("HYDRAR_CLIENT"),
  sparkHome = Sys.getenv("SPARK_HOME"),
  sparkConfig = list("spark.driver.memory" = Sys.getenv("HYDRAR_SPARK_DRIVER_MEMORY")),
  ...
  ) {
  logSource <- "hydrar.session"
  #@TODO in the future make the signature of this function match sparkr.session()
  
  if (hydrar.env$HYDRAR_SESSION_EXISTS) {
    hydrar.warn(logSource, "HydraR session already initialized")
    return()
  }

  if (nchar(master) == 0) {
    hydrar.warn(logSource, "master not defined. Defaulting to local[*]")
    master <- "local[*]"
  }
  
  if (nchar(sparkHome) == 0) {
    hydrar.err(logSource, "SPARK_HOME not defined")
  }
  
  if (length(sparkConfig$spark.driver.memory) == 0  || nchar(sparkConfig$spark.driver.memory) == 0) {
    hydrar.warn(logSource, "driver.memory not defined. Defaulting to 2G")
    sparkConfig$spark.driver.memory <- "2G"
  }
  
  sc <- SparkR::sparkR.session(
    master = master,
    appName = "HydraR",
    sparkHome = sparkHome,
    sparkConfig = sparkConfig,
    sparkJars = hydrar.env$SYSML_JARS(),
    sparkPackages = "",
    ...)
  
  sysml.init(sc)
  
  hydrar.env$HYDRAR_SESSION_EXISTS <- TRUE
  
}

#' hydrar.session.stop
#' @description Stop HydraR
#' @export
hydrar.session.stop <- function() {
  logSource <- "hydrar.session.stop"
  
  if(hydrar.env$HYDRAR_SESSION_EXISTS == FALSE) {
    hydrar.warn(logSource, "No HydraR session exists")
    return()
  }
  
  SparkR::sparkR.session.stop()
  
  sysml.stop()
  
  hydrar.env$HYDRAR_SESSION_EXISTS <- FALSE
  
}
