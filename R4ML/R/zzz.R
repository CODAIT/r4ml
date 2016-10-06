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

# this will be called at the library loading time.
# it first check if the sparkR packages is loaded?
#   (which one must before using HydraR)
# the it initialize SparkR basically starting the cluster and setting the
# internals of sparkR with extra loaded jar from systemML so that one
# call the systemML as well as SparkR functionality as needed
# if sparkR is already initialized then it wil re-initialize it
.onLoad <- function(libname, pkgname) {

  if ("SparkR" %in% loadedNamespaces()) {
    if (exists(".sparkRjsc", envir = SparkR:::.sparkREnv)) {
      hydrar.reload.SparkR()
      stop("Reloading SparkR")
    } else {
      hydrar.load.SparkR()
    }
  }


  hydrar.init()
}

# This function is called at the unloading of the HydraR library or at the time
# of detaching it.
# it basically stops the SparkR cluster and unload any namespace associated with it
#
.onUnload <- function(libpath) {
  hydrar.unload.SparkR()
}
# a utlity function to check is sparkR loaded
is.sparkr.loaded <- function() {
  if ("SparkR" %in% loadedNamespaces()) {
    return (TRUE)
  } else {
    return (FALSE)
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

# first stop the SparkR cluster and detach it
# then restart SparkR with systemML and attach it to env
hydrar.reload.SparkR <- function() {
  hydrar.unload.SparkR()
  hydrar.load.SparkR()
}


#load and initialize SparkR
hydrar.load.SparkR <- function() {

  {
    # check:begin SPARK_HOME
    # try to see if user has set the SPARK_HOME
    if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
      stop("Must have the SPARK_HOME env variable set or RLIBS in ~/.Renviron")
    }
    # check:end SPARK_HOME
  }

  {
    # check:begin SparkR loading
    if (!requireNamespace("SparkR")) {
      warning("SparkR not found in the standard library or in the R_LIBS path. Consider rechecking your ~/.Renviron file")
  
      lib_loc = file.path(Sys.getenv("SPARK_HOME"), "R", "lib")
      .libPaths(c(lib_loc, .libPaths()))
      #library(SparkR, lib.loc = c(lib_loc))
      requireNameSpace("SparkR")
    }
    # check:end SparkR loading
  }

  {
    # check:begin SystemML jars
    #start the SparkR shell and initialization default
    sysml_jars <- file.path(system.file(package="HydraR"), "lib", "SystemML.jar")

    if (nchar(Sys.getenv("SYSML_HOME")) >= 1) {
      # if user has set the env then we can use that jar
      sysml_jars <- file.path(Sys.getenv("SYSML_HOME"), "target", "SystemML.jar")
    }

    # highest priority given to HYDRAR_SYSML_JAR var set. (it might be used in future)
    if (nchar(Sys.getenv("HYDRAR_SYSML_JAR")) >= 1) {
      # if user has set the env then we can use that jar
      sysml_jars <- Sys.getenv("HYDRAR_SYSML_JAR")
    }

    if (!file.exists(sysml_jars)) {
      stop("ERROR: can't find the SystemML.jar for initialization")
    }
    # check:end SystemML jars
  }
  
  {
    # check:begin HYDRAR_CLIENT
    if (nchar(Sys.getenv("HYDRAR_CLIENT")) >= 1) {
      hydrar_client <- Sys.getenv("HYDRAR_CLIENT")
    } else {
      warning("HYDRAR_CLIENT not defined in the .Renviron file. Defaulting to local[*]")
      hydrar_client <- "local[*]"
    }
    # check:end HYDRAR_CLIENT
  }
  
  
  {


    # create: begin SparkSession
    # note that the name is the misnomer and should be sparksession
    if (nchar(Sys.getenv("HYDRAR_SPARK_DRIVER_MEMORY")) >= 1) {
      spark_driver_memory <- Sys.getenv("HYDRAR_SPARK_DRIVER_MEMORY")
      } else {
        warning("HYDRAR_SPARK_DRIVER_MEMORY not defined in the .Renviron file. Defaulting to \"2g\"")
        spark_driver_memory <- "2g"
      }
    sc <- SparkR::sparkR.session(
      master = hydrar_client,
      sparkConfig = list(spark.driver.memory = spark_driver_memory),
      sparkJars = sysml_jars,
      enableHiveSupport = FALSE, # seems like it's mandatory for now as of spark2.0
    )

    #spark context is now replace by sparkSession
    assign("sc", sc, envir = .GlobalEnv)
    # create: end SparkSession
  }

  {
    # create: begin SysmlSparkContext
    # we get the sparkContext as in the systemML most of the code uses the sparkContext
    sysmlSparkContext <- SparkR:::callJMethod(sc, "sparkContext")
    assign("sysmlSparkContext", sysmlSparkContext, .GlobalEnv)
    # create: end SysmlSparkContext
  }

  {
    # create: begin SysmlSqlContext
    # since sysmlSqlContext is used for most of the systemML related
    # code we need to have it.
    sysmlSqlContext <- SparkR:::callJMethod(sc, "sqlContext")
    assign("sysmlSqlContext", sysmlSqlContext, .GlobalEnv)
    # create: end SysmlSqlContext
  }

}

#Initialize HydraR
hydrar.init <- function() {
  #logger and level settings
  logger <- log4j.Logger$new("org")
  assign("logger", logger, .GlobalEnv)

  logger <- log4j.Logger$new()
  assign("logger", logger, .GlobalEnv)
  logger$setLevel("WARN")

  #static class sysml.RDDUtils
  # since actual class is static, we need this
  sysml.RDDUtils = sysml.RDDConverterUtils$new()
  assign("sysml.RDDUtils", sysml.RDDUtils, .GlobalEnv)
}

# unload SparkR and claim the cluster and cleanup
hydrar.unload.SparkR <- function() {
  SparkR::sparkR.stop()
  detach_package("SparkR")
}

#' detach the package recursively (even if multiple version exists)
#'
#' It is the wrapper over the base detach package
#'
#' @examples
#' detach_package(HydraR)
#'
#' @export
detach_package <- function(pkg, character.only = FALSE)
{
  if(!character.only) {
    pkg <- deparse(substitute(pkg))
  }
  search_item <- paste("package", pkg, sep = ":")
  while(search_item %in% search()) {
    detach(search_item, unload = TRUE, character.only = TRUE)
  }
}
