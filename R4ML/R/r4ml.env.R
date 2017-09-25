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
message("Creating R4ML environment")

# set the global environment to be used along with other codes
#' @export
r4ml.env <- new.env()

# assign the global variables in the package environment
with(r4ml.env, {
  # This package's name
  PACKAGE_NAME <- "R4ML"

  # LOG LEVEL constants
  DEFAULT_LOG_LEVEL <- "INFO"
  LOG_LEVEL <- DEFAULT_LOG_LEVEL
  
  # the log level when dml is executing
  SYSML_LOG_LEVEL <- "ERROR"

  VERBOSE <- FALSE # for the more verbose output from R4ML
  
  R4ML_SESSION_EXISTS <- FALSE

  EMPTY_STRING_RECODE <- "r4ml::__empty_string__"

  # Internall SystemML can reshuffle the row maintain the global index for order
  SYSML_MATRIX_INDEX_COL <- "__INDEX"

  # The number of rows returned by head and tail methods
  DEFAULT_HEAD_ROWS <- 6

  # The number of rows returned by show method
  DEFAULT_SHOW_ROWS <- 20

  DML_DATA_TYPES <- c("scale", "nominal", "ordinal", "dummy")

  # DML script names
  DML_UNIVARIATE_STATS_SCRIPT <- "Univar-Stats.dml"
  DML_BIVARIATE_STATS_SCRIPT <- "bivar-stats.dml"
  DML_STRAT_BIVARIATE_STATS_SCRIPT <- "stratstats.dml"
  DML_PCA_SCRIPT <- "PCA.dml"
  DML_M_SVM_SCRIPT <- "m-svm.dml"
  DML_L2_SVM_SCRIPT <- "l2-svm.dml"
  DML_M_SVM_TEST_SCRIPT <- "m-svm-predict.dml"
  DML_L2_SVM_TEST_SCRIPT <- "l2-svm-predict.dml"
  DML_NAIVE_BAYES_SCRIPT <- "naive-bayes.dml"
  DML_NAIVE_BAYES_TEST_SCRIPT <- "naive-bayes-predict.dml"
  DML_MULTI_LOGISTIC_REGRESSION_SCRIPT <- "MultiLogReg.dml"
  DML_GLM_SCRIPT <- "GLM.dml"
  DML_GLM_PREDICT_SCRIPT <- "GLM-predict.dml"
  DML_GLM_TEST_SCRIPT <- "GLM-predict.dml"
  DML_KMEANS_SCRIPT <- "Kmeans.dml"
  DML_KMEANS_TEST_SCRIPT <- "Kmeans-score.dml"
  DML_LM_DS_SCRIPT <- "LinearRegDS.dml"
  DML_LM_CG_SCRIPT <- "LinearRegCG.dml"
  DML_DECISION_TREE_SCIPT <- "decision-tree.dml"
  DML_DECISION_TREE_TEST_SCRIPT <- "decision-tree-predict.dml"
  DML_RANDOM_FOREST_SCRIPT <- "random-forest.dml"
  DML_RANDOM_FOREST_TEST_SCRIPT <- "random-forest-predict.dml"
  DML_ALS_SCRIPT <- "ALS.dml"
  DML_WRITE_CSV2BIN <- file.path(".", "utils", "csv2bin.dml")
  DML_ALS_DS_SCRIPT <- "ALS-DS.dml"
  DML_ALS_PREDICT_SCRIPT <- "ALS-predict.dml"
  DML_ALS_TOP_PREDICT_SCRIPT <- "ALS_topk_predict.dml"
  DML_KM_SCRIPT <- "KM.dml"
  DML_COX_SCRIPT <- "Cox.dml"
  DML_COX_PREDICT_SCRIPT <- "Cox-predict.dml"
  UNIVARIATE_STATS_SUFFIX <- ".univar.stats"
  BIVARIATE_STATS_SUFFIX <- ".bivar.stats"
  STRAT_BIVARIATE_STATS_SUFFIX <- ".strat.stats"
  DML_TYPES_SUFFIX <- ".types"
  DML_COLS_SUFFIX <- ".cols"
  ALS_PAIR <- "pair"
  ALS_TOPK <- "topk"
  FBP_SUFFIX <- ".factorization.based.predict"
  DML_KM_MODEL <- file.path("", "km.model")
  DML_KM_MODEL_OFFSET <- file.path("", "km.model.offset")
  DML_KM_MEDIANCONFINTERVAL <- file.path("", "km.median.conf.intervals")
  DML_KM_GROUPS <- file.path("", "km.groups")
  DML_KM_STRATUM <- file.path("", "km.stratum")
  DML_TRANSFORM_SCRIPT <- "transform_old.dml"
  DML_APPLY_TRANSFORM_SCRIPT <- "apply-transform_old.dml"
  DOT_MAP <- ".map"
  ACCURACY <- file.path("", "accuracy.csv")
  MODEL_METADATA <- file.path("", "model.json")
  DEBUG_LOG <- file.path("", "debug.log")

  #SVM specific constants
  COEFFICIENTS <- file.path("", "coefficients.csv")
  INTERCEPT <- "(Intercept)"
  CONDITIONALS <- file.path("", "conditionals.csv")
  CSV <- "csv"

  # the minimum partition size in bytes for automatic repartitioning
  MIN_PARTITION_SIZE <- 128000000
  
  # the min size of an object that we will repartion
  MIN_REPARTION_SIZE <- 10000

}) # End with

# define the global utility function in the r4ml.env
with(r4ml.env, {

  # utility to find SystemML script locations
  SYSML_SCRIPT_ROOT <- function() {
    root <- Sys.getenv("SYSML_HOME")
    if (is.null(root) || root == "") {
      root <- system.file("sysml", package="R4ML")
    } else {
      root <- file.path(c(root, "scripts"))
    }
    spath <- file.path(root, "scripts")
    if (!file.exists(spath)) {
      stop("Unable to locate SystemML scripts. (hint: set environment variable SYSML_HOME)")
    }
    spath
  }

  #utility to find the location of the dml algorithms
  SYSML_ALGO_ROOT <- function() {
    path <- file.path(SYSML_SCRIPT_ROOT(), "algorithms")
    if (!file.exists(path)) {
      stop("Unable to locate SystemML DML scripts")
    }
    return (path)
  }
  
  SYSML_JARS <- function() {
    sysml_jars <- file.path(system.file(package="R4ML"), "java", "SystemML.jar")
    
    if (nchar(Sys.getenv("SYSML_HOME")) >= 1) {
      sysml_jars <- file.path(Sys.getenv("SYSML_HOME"), "target", "SystemML.jar")
    }

    if (nchar(Sys.getenv("R4ML_SYSML_JAR")) >= 1) {
      sysml_jars <- Sys.getenv("R4ML_SYSML_JAR")
      message("using custom SystemML jar: ", sysml_jars)
    }

    if (!file.exists(sysml_jars)) {
      stop("Unable to locate SystemML.jar. Set the location via environmental varible R4ML_SYSML_JAR")
    }
  
    return(sysml_jars)
}

  
  # utility to find the location of the scratch workspace
  # this location is used for temporaty storage
  WORKSPACE_ROOT <- function(subdir="") {
    fs_root <- r4ml.env$r4ml.fs$user.home()
    ws_root <- file.path(fs_root, PACKAGE_NAME, "scratch_workspace", subdir)
    workspace <- r4ml.env$r4ml.fs$tempdir(prefix = ws_root)
    return(workspace)
  }
  
  
  TESTTHAT_LONGTEST <- function() {
    # if TRUE unit test that take a long time will be run
    if (Sys.getenv("R4ML_TESTTHAT_LONGTEST") == "1") {
      return(TRUE)
    }
    
    return(FALSE)
  }

  TESTTHAT_EXAMPLES <- function() {
    # if TRUE unit test that take a long time will be run
    if (Sys.getenv("R4ML_TESTTHAT_EXAMPLES") == "1") {
      return(TRUE)
    }
  
    return(FALSE)
  }
})
