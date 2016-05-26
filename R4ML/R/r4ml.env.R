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
cat("Attaching...")

# set the global environment to be used along with other codes
#' @export
hydrar.env <- new.env()

# assign the global variables in the package environment
with(hydrar.env, {
  # This package's name
  PACKAGE_NAME <- "hydrar";

  # LOG LEVEL constants
  INFO_LOG_LEVEL <- 4;
  WARN_LOG_LEVEL <- 2;
  ERR_LOG_LEVEL <- 1;
  NO_LOG_LEVEL <- 0;
  DEFAULT_LOG_LEVEL <- ERR_LOG_LEVEL + WARN_LOG_LEVEL;
  LOG_LEVEL <- DEFAULT_LOG_LEVEL;

  # hadoop fs
  HYDRAR_DEFAULT_FS <- "";

  # hydrar.dataset constants
  # Possible data sources
  TEXT_FILE <- "DEL";
  DEL <- "DEL";
  LINE_FILE <- "LINE"
  JSON <- "JSON";
  TRANSFORM <- "TRANSF"; # When a hydrar.dataset is the result of a transformation (e.g., projection or filtering)

  EMPTY_TABLE_EXPRESSION <- "[[]]";

  # A list of supported data formats
  DATA_SOURCES <- c(TEXT_FILE, JSON, TRANSFORM, LINE_FILE);

  # Supported data types
  DATA_TYPES <- c(character="string",
                  character="varchar",
                  character="timestamp",
                  character="array",
                  numeric="double",
                  numeric="float",
                  numeric="real",
                  numeric="numeric",
                  numeric="decimal",
                  numeric="bigint",
                  integer="long",
                  integer="tinyint",
                  integer="smallint",
                  integer="int",
                  logical="boolean",
                  factor="string")

  SUPPORTED_DATA_TYPES <- c("character", "numeric", "integer", "logical");

  #
  SYSML_BLOCK_MATRIX_SIZE <- list("nrows" = 10000, "ncols" = 10000)

  # The number of rows returned by head and tail methods
  DEFAULT_HEAD_ROWS <- 6;

  # The default delimiter for text files
  DEFAULT_DELIMITER <- ",";

  # NA string
  DEFAULT_NA_STRING <- "";

  # Local processing
  DEFAULT_LOCAL_PROCESSING <- TRUE;

  # If no rows are returned by a query
  #EMPTY_DATA <- data.frame();

    # hydrar.vector constants

  # hydrar.frame constants

  # The default column names if none specified
  DEFAULT_COLNAMES <- c("V1");

  UNKNOWN_COLNAMES <-1;

  # The default column types if none specified
  DEFAULT_COLTYPES <- c("character");

  DEFAULT_NBINS = 10;

  # Aggregate functions for numeric columns
  ALL_AGGREGATE_FUNCTIONS = c("count", "countNA", "countnonNA", "min", "max", "sum", "avg", "mean", "sd", "var");

  RESERVED_WORDS = c(ALL_AGGREGATE_FUNCTIONS);

  ALL_NOMINAL_AGGREGATE_FUNCTIONS = c("count", "countNA", "countnonNA", "min", "max");

  # Default aggregate functions for numeric columns
  DEFAULT_NUMERIC_AGGREGATE_FUNCTIONS = c("countnonNA", "min", "max", "sum", "mean");

  # Aggregate functions for nominal columns
  DEFAULT_NOMINAL_AGGREGATE_FUNCTIONS = c("countnonNA", "min", "max");

  # The list of aggregate functions which always return a numeric value
  NUMERIC_TYPE_AGGREGATE_FUNCTIONS = c("count", "sum", "mean", "countNA", "countnonNA");

  ALL_COLUMNS = ".";

  # hydrar.function constants


  REGISTERED_FUNCTIONS <- list();


  # hydrar.sampling constants
  RANDOM_SEED <- 71

  # warnings and errors log file
  LOGDIR <- "";
  LOGFILE <- "";
  LOGCOLTYPE <- c();
  LOGCOLNAME <- c();

  # Random numbers / RNG support
  #DEFAULT_RNG <- "hydrar.default.rng";

  # A list of temporary files to be removed by the finalizers
  TEMP_FILES <- list();

  # A list of temporary files to be removed when the exit from hydrar.err
  DELETE_ON_ERROR_FILES <- list();


  # Default local R temp directory
  DEFAULT_R_TMP_DIR <- "/tmp/hydrar"

  # default DML path
  DML_ALGORITHMS_PATH <- "algorithms/";

  DML_DATA_TYPES <- c("scale", "nominal", "ordinal", "dummy")
  # DML script names
  DML_UNIVARIATE_STATS_SCRIPT <- "Univar-Stats.dml";
  DML_BIVARIATE_STATS_SCRIPT <- "bivar-stats.dml";
  DML_STRAT_BIVARIATE_STATS_SCRIPT <- "stratstats.dml";
  DML_PCA_SCRIPT <- "PCA.dml";

  DML_M_SVM_SCRIPT <- "m-svm.dml";
  DML_L2_SVM_SCRIPT <- "l2-svm.dml";
  DML_M_SVM_TEST_SCRIPT <- "m-svm-predict.dml";
  DML_L2_SVM_TEST_SCRIPT <- "l2-svm-predict.dml";
  DML_NAIVE_BAYES_SCRIPT <- "naive-bayes.dml";
  DML_NAIVE_BAYES_TEST_SCRIPT <- "naive-bayes-predict.dml";
  DML_MULTI_LOGISTIC_REGRESSION_SCRIPT <- "MultiLogReg.dml"
  DML_GLM_SCRIPT <- "GLM.dml";
  DML_GLM_PREDICT_SCRIPT <- "GLM-predict.dml";
  DML_GLM_TEST_SCRIPT <- "GLM-predict.dml";
  DML_KMEANS_SCRIPT <- "Kmeans.dml"
  DML_KMEANS_TEST_SCRIPT <- "Kmeans-score.dml"
  DML_LM_DS_SCRIPT <- "LinearRegDS.dml";
  DML_LM_CG_SCRIPT <- "LinearRegCG.dml";
  DML_DECISION_TREE_SCIPT <- "decision-tree.dml";
  DML_DECISION_TREE_TEST_SCRIPT <- "decision-tree-predict.dml";
  DML_RANDOM_FOREST_SCRIPT <- "random-forest.dml";
  DML_RANDOM_FOREST_TEST_SCRIPT <- "random-forest-predict.dml"
  DML_ALS_SCRIPT <- "ALS.dml";
  DML_ALS_PREDICT_SCRIPT <- "ALS_predict.dml";
  DML_WRITE_CSV2BIN <- "./utils/csv2bin.dml";
  DML_ALS_TOP_PREDICT_SCRIPT <- "ALS_topk_predict.dml";
  DML_KM_SCRIPT <- "KM.dml";
  DML_COX_SCRIPT <- "Cox.dml";
  DML_COX_PREDICT_SCRIPT <- "Cox-predict.dml";

  DML_SCALE_TYPE = 1;
  DML_NOMINAL_TYPE = 2;

  DML_UNIVARIATE_STATS_LIST <- c("Min.", "Max.", "Range", "Mean", "Var", "SD", "SEM", "CoV",
                                 "Skewness", "Kurtosis", "SES", "SEK", "Median", "IQM", "# cat.", "Mode", "# modes");

  DML_BIVARIATE_STATS_NN <- c("Chi_sq", "DF", "P_value", "Cramer's_V") ;
  DML_BIVARIATE_STATS_NS <- c("Eta", "F_statistic", "P_value", "SumOfSquares_bw", "SumOfSquares_within", "DF_bw", "DF_within", "Mean_Sq_bw", "Mean_Sq_within");
  DML_BIVARIATE_STATS_SS <- c("Pearson's_r", "Covar", "SD(X)", "SD(Y)");
  DML_BIVARIATE_STATS_OO <- c("Spearman's rank cor. coeff.");
  UNIVARIATE_STATS_SUFFIX <- ".univar.stats";
  BIVARIATE_STATS_SUFFIX <- ".bivar.stats";
  STRAT_BIVARIATE_STATS_SUFFIX <- ".strat.stats";
  DML_TYPES_SUFFIX <- ".types";
  DML_COLS_SUFFIX <- ".cols";

  ALS_PAIR <- "pair";
  ALS_TOPK <- "topk";
  FBP_SUFFIX <- ".factorization.based.predict";

  DML_KM_MODEL <- "/km.model";
  DML_KM_MODEL_OFFSET <- "/km.model.offset";
  DML_KM_MEDIANCONFINTERVAL <- "/km.median.conf.intervals";
  DML_KM_TESTS <- "/km.tests";
  DML_KM_TESTS_GRPS_OE_SUFFIX <- ".groups.oe";
  DML_KM_GROUPS <- "/km.groups";
  DML_KM_STRATUM <- "/km.stratum";

  DML_TRANSFORM_SCRIPT <- "transform_old.dml";
  DML_APPLY_TRANSFORM_SCRIPT <- "apply-transform_old.dml"

  DML_MATRIX_FORMAT_BINARY <- "binary";
  DML_MATRIX_FORMAT_CSV <- "csv";

  SPLIT_DUMMYCODE_MAPS <- "/splitDummyCodeMaps.csv";


  TRANSFORM_INFO <- "/transform.info";
  DOT_MAP <- ".map";
  DOT_BIN <- ".bin";
  X_MTX <- "x.mtx";
  Y_MTX <- "y.mtx";
  ACCURACY <- "/accuracy.csv";
  MODEL_METADATA <- "/model.json";
  DEBUG_LOG <- "/debug.log";

  #SVM specific constants
  COEFFICIENTS <- "/coefficients.csv";
  SUPPORT_VECTORS <- "support_vectors.csv";
  RECODE_MAPS <- "/recode_maps";

  PREDICTIONS <- "/predictions.csv";
  CONFUSION <- "/confusion.csv";
  SCORES <- "/scores.csv";
  INTERCEPT <- "(Intercept)";

  DEFAULT_LAPLACE_CORRECTION <- 1;
  PRIOR <- "/prior.csv";
  CONDITIONALS <- "/conditionals.csv";
  CSV <- "csv";

  DECISIONTREE <- "/decisiontree.csv";

  RANDOMFOREST <- "/randomforest.csv";
  RANDOM_FOREST_COUNT <- "/counts.csv";
  RANDOM_FOREST_OOB <- "/OOB.csv";

  PROBABILITIES <- "/probabilities.csv";

  #Logistic Regression specific constants
  BETA <- "/beta.csv";
  STATISTICS <- "/statistics.csv";

}) # End with

# define the global utility function in the hydrar.env
with(hydrar.env, {

  # utility to find SystemML script locations
  SYSML_SCRIPT_ROOT <- function() {
    root <- Sys.getenv("SYSML_HOME")
    if (is.null(root) || root == "") {
      root <- system.file("sysml", package="HydraR")
    } else {
      root <- file.path(c(root, "scripts"));
    }
    spath <- file.path(root, "scripts")
    if (!file.exists(spath)) {
      stop("Can't find the systemML scripts")
    }
    spath
  };

  #utility to find the location of the dml algorithms
  SYSML_ALGO_ROOT <- function() {
    path <- file.path(SYSML_SCRIPT_ROOT(), "algorithms")
    if (!file.exists(path)) {
      stop("Can't find the systemML algorithm scripts")
    }
    path
  };

  #utility to find the location of the scratch workspace
  # this location is used for temporaty storage
  WORKSPACE_ROOT <- function(subdir="") {
    home <- Sys.getenv("HOME")
    if (is.null(home) || home == "") {
      stop("User HOME env not found")
    }
    workspace <- file.path(home, PACKAGE_NAME, "scratch_workspace", subdir);
    if (!file.exists(workspace)) {
      system(paste0("mkdir -p ", workspace))
    }
    workspace
  }
})
