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
#' @include zzz.R
NULL

requireNamespace("SparkR")

#' @name sysml.MatrixCharacteristics
#' @title
#' A Reference Class that represent systemML MatrixCharacteristics
#' @description 
#' Create a meta info about the block and overall size of matrix
#' This class acts as the metadata for the systemML matrix and is used by other
#' classes in the systemML.
#'
#' @family MLContext functions
#'
#' @field env An R environment that stores bookkeeping states of the class,
#'        along with java reference corresponding to the JVM.
#' @export
#' @examples \dontrun{
#' sysml.MatrixCharacteristics()
#' }
#'

sysml.MatrixCharacteristics <- setRefClass("sysml.MatrixCharacteristics",
  fields = list(env = "environment"),
  methods = list(
    initialize = function() {
      env <<- new.env()
      env$jref <<- SparkR::sparkR.newJObject("org.apache.sysml.runtime.matrix.MatrixCharacteristics")
    },
    finalize = function() {
      SparkR:::cleanup.jobj(env$jref)
    }
  )

)

#'
#' A Reference Class that represent systemML MLContext
#'
#' MLContext is the gateway to the systemML world and this class provides
#' ability of the R code to connect and use the various systemML
#' high performant distributed linear algebra library and allow use
#' to run the dml script in the R code.
#'
#' @family MLContext functions
#'
#' @field env An R environment that stores bookkeeping states of the class,
#'        along with java reference corresponding to the JVM.
#' @export
#' @examples \dontrun{
#'    #sysmlSparkContext # the default spark context
#'    mlCtx = R4ML:::sysml.MLContext$new(sysmlSparkContext)
#' }
sysml.MLContext <- setRefClass("sysml.MLContext",
  fields = list(env="environment"),
  methods = list(
    initialize = function(sparkContext = sysmlSparkContext) {
      env <<- new.env()
      if (missing(sparkContext)) {
        sparkContext = get("sysmlSparkContext", .GlobalEnv)
      }
      env$jref <<- SparkR:::newJObject("org.apache.sysml.api.MLContext", sparkContext)
    },

    finalize = function() {
      SparkR:::cleanup.jobj(env$jref)
    },

    reset = function() {
      '\\tabular{ll}{
         Description:\\tab \\cr
           \\tab reset the MLContext \\cr
       }'
      SparkR:::callJMethod(env$jref, "reset")
    },

    registerInput = function(dmlname, rdd_or_df, mc) {
      logSource <- "registerInput"
      '\\tabular{ll}{
         Description:\\tab \\cr
           \\tab bind the rdd or dataframe to the dml variable
       }
       \\tabular{lll}{
         Arguments:\\tab \\tab \\cr
           \\tab dmlname \\tab variable name in the dml script \\cr
           \\tab rdd_or_df \\tab SparkR:::RDD rdd class which need to be attached\\cr
           \\tab mc \\tab \\link{sysml.MatrixCharacteristics} info about block matrix
       }
      '
      #check the args
      stopifnot(class(dmlname) == "character",
                class(mc) == "sysml.MatrixCharacteristics")
      # check rdd_or_df arg
      cls <- as.vector(class(rdd_or_df))
      jrdd <- NULL
      if (cls == 'SparkDataFrame') {
        jrdd <- SparkR:::toRDD(rdd_or_df)
      } else if  (cls == 'RDD' || cls == 'PipelinedRDD') {
        jrdd <- SparkR:::getJRDD(rdd_or_df)
      } else if (cls == 'jobj') {
        jrdd = rdd_or_df
      } else {
        r4ml.err(logSource, "unsupported argument rdd_or_df only rdd or dataframe is supported")
      }

      SparkR:::callJMethod(env$jref, "registerInput", dmlname, jrdd, mc$env$jref)
    },

    registerOutput = function(dmlname) {
      '\\tabular{ll}{
         Description:\\tab \\cr
           \\tab bind the output dml var
       }
       \\tabular{lll}{
         Arguments:\\tab \\tab \\cr
           \\tab dmlname \\tab variable name in the dml script \\cr
         }
      '
      stopifnot(class(dmlname) == "character")
      SparkR:::callJMethod(env$jref, "registerOutput", dmlname)
    },

    executeScriptNoArgs = function(dml_script) {
      '\\tabular{ll}{
        Description:\\tab \\cr
        \\tab This is deprecated. execute the string containing the dml code
      }
      \\tabular{lll}{
      Arguments:\\tab \\tab \\cr
      \\tab dml_script \\tab string containing the dml script whose variables has been bound using registerInput and registerOut \\cr
      \\tab rdd_or_df \\tab SparkR:::RDD rdd class which need to be attached\\cr
      \\tab mc \\tab \\link{sysml.MatrixCharacteristics} info about block matrix

      }
      '
      stopifnot(class(dml_script) == "character")
      out_jref <- SparkR:::callJMethod(env$jref, "executeScript", dml_script)
      #@TODO. get sysmlSqlContext from the ctor
      outputs <- sysml.MLOutput$new(out_jref, sysmlSqlContext)
    },

    executeScriptBase = function(dml_script, arg_keys, arg_vals, is.file) {
      '\\tabular{ll}{
      Description:\\tab \\cr
      \\tab execute the string containing the dml code
      }
      \\tabular{lll}{
      Arguments:\\tab \\tab \\cr
      \\tab dml_script \\tab string containing the dml script whose variables has been bound using registerInput and registerOut \\cr
      }
      '
      stopifnot(class(dml_script) == "character")

      is_namedargs = FALSE
      if (!missing(arg_keys) && !missing(arg_vals)) {
        stopifnot(length(arg_keys) == length(arg_vals))
        if (length(arg_keys) > 0) {
          is_namedargs = TRUE
        }
      }
      # create keys
      jarg_keys <- java.ArrayList$new()
      if (!missing(arg_keys)) {
        sapply(arg_keys, function (e) {
          jarg_keys$add(e)
        })
      }

      #create vals
      jarg_vals <- java.ArrayList$new()
      if (!missing(arg_vals)) {
        sapply(arg_vals, function (e) {
          jarg_vals$add(e)
        })
      }
      #DEBUG browser()
      out_jref <- NULL
      
      previous_log_level <- jlogger$getLevel()
      invisible(jlogger$setLevel(r4ml.env$SYSML_LOG_LEVEL))
      
      if (is.file) {
        if (is_namedargs) {
          out_jref <- SparkR:::callJMethod(
                        env$jref, "execute",
                        dml_script,
                        jarg_keys$env$jref,
                        jarg_vals$env$jref
                      )
        } else {
          out_jref <- SparkR:::callJMethod(
                        env$jref, "execute",
                        dml_script
                      )
        }
      } else {
        if (is_namedargs) {
          out_jref <- SparkR:::callJMethod(
                        env$jref, "executeScript",
                        dml_script,
                        jarg_keys$env$jref,
                        jarg_vals$env$jref
                       )
        } else {
          out_jref <- SparkR:::callJMethod(
                        env$jref, "executeScript",
                        dml_script
                      )
        }
      }
      
      invisible(jlogger$setLevel(previous_log_level))
      
      #@TODO. get sysmlSqlContext from the ctor
      outputs <- sysml.MLOutput$new(out_jref, sysmlSqlContext)
    },

    executeScript = function(dml_script, arg_keys, arg_vals) {
      '\\tabular{ll}{
      Description:\\tab \\cr
      \\tab execute the string containing the dml code
      }
      \\tabular{lll}{
      Arguments:\\tab \\tab \\cr
      \\tab dml_script \\tab string containing the dml script whose variables has been bound using registerInput and registerOut \\cr
      \\tab arg_keys \\tab arguement name of the dml script\\cr
      \\tab arg_vals \\tab corresponding arguement value of the dml scripts.
      }
      '
      .self$executeScriptBase(dml_script, arg_keys, arg_vals, is.file=FALSE)
    },

    execute = function(dml_script, arg_keys, arg_vals) {
      '\\tabular{ll}{
      Description:\\tab \\cr
      \\tab execute the string containing the dml code
      }
      \\tabular{lll}{
      Arguments:\\tab \\tab \\cr
      \\tab dml_script \\tab string containing the dml script whose variables has been bound using registerInput and registerOut \\cr
      \\tab arg_keys \\tab arguement name of the dml script\\cr
      \\tab arg_vals \\tab corresponding arguement value of the dml scripts.
      }
      '
      .self$executeScriptBase(dml_script, arg_keys, arg_vals, is.file=TRUE)
    }
  )
)

#'
#' A Reference Class that represent systemML MLContext
#'
#' MLContext is the gateway to the systemML world and this class provides
#' ability of the R code to connect and use the various systemML
#' high performant distributed linear algebra library and allow use
#' to run the dml script in the R code.
#'
#' @family MLContext functions
#'
#' @field env An R environment that stores bookkeeping states of the class,
#'        along with java reference corresponding to the JVM.
#' @export
#' @examples \dontrun{
#'    #TODO fix this example
#'    #sysmlSparkContext # the default spark context
#'    #mlCtx = sysml.MLOutput$new()
#' }
sysml.MLOutput <- setRefClass("sysml.MLOutput",
  fields = list(env="environment"),
  methods = list(
    initialize = function(jref, sysmlSqlContext) {
      if (missing(sysmlSqlContext)) {
        sysmlSqlContext = get("sysmlSqlContext", .GlobalEnv)
      }

      if (missing(jref)) {
        stop("Must have jref object in the ctor of MLOutput")
      }
      env <<- new.env()
      env$jref <<- jref
      env$sysmlSqlContext <<- sysmlSqlContext
    },

    finalize = function() {
      SparkR:::cleanup.jobj(env$jref)
    },

    getDF = function(colname, drop.id = TRUE) {
      '\\tabular{ll}{
      Description:\\tab \\cr
      \\tab get the sparkR dataframe from MLOutput \\cr
      \\tab drop.id (default = TRUE) whether to drop internal columns ID\\cr
      }'
      stopifnot(class(colname) == "character")
      df_jref <- SparkR:::callJMethod(env$jref, "getDF", env$sysmlSqlContext, colname)

      df_unsorted <- new("SparkDataFrame", sdf=df_jref, isCached=FALSE)
      # since sysML create the extra internal __INDEX column, which is used to 
      # manage global order we do the sort sort wrt to _INDEX first
      # note that SparkR::arrange i.e spark.sort is efficiency implemented using merge
      # algo
      index_col <- r4ml.env$SYSML_MATRIX_INDEX_COL
      df <- SparkR::arrange(df_unsorted, index_col)

      # note that the sysml internal order col is not needed by user and hence
      # drop the index id
      # rename the remaining column to 'colname'
      oldnames <- SparkR::colnames(df)
      no_ids <- oldnames[oldnames != index_col]
      df_noid <- SparkR:::select(df, no_ids)
      newnames <- as.vector(sapply(no_ids, function(x) colname))
      SparkR::colnames(df_noid) <- newnames
      df_noid

    }
  )
)

#'
#' A Reference Class that represent systemML RDDConverterUtils and RDDConverterUtilsExt
#'
#' RDDConvertUtils lets one transform various RDD related info into the systemML internals
#' BinaryBlockRDD
#'
#' @family MLContext functions
#'
#' @field env An R environment that stores bookkeeping states of the class,
#'        along with the java reference corresponding to the JVM.
#' @export
#' @examples \dontrun{
#' airr <- R4ML::airline
#' airrt <- airr$Distance
#' airrt[is.na(airrt)] <- 0
#' airrtd <- as.data.frame(airrt)
#' air_dist <- createDataFrame(airrtd)
#' 
#' X_cnt <- SparkR::count(air_dist)
#' X_mc <- R4ML:::sysml.MatrixCharacteristics$new(X_cnt, 1, 10, 1)
#' rdd_utils <- R4ML:::sysml.RDDConverterUtils$new()
#' air_dist <- as.r4ml.matrix(air_dist)
#' bb_df <- rdd_utils$dataFrameToBinaryBlock(air_dist, X_mc)
#' }
#'
sysml.RDDConverterUtils <- setRefClass("sysml.RDDConverterUtils",
  fields = list(env="environment"),
  methods = list(
    initialize = function(sparkContext) {
      if (missing(sparkContext)) {
        sparkContext = get("sysmlSparkContext", .GlobalEnv)
      }
      env <<- new.env()
      env$sparkContext <<- sparkContext

      # Some of the deprecated methods we call require JavaSparkContext.
      env$javaSparkContext <<- 
          SparkR:::newJObject("org.apache.spark.api.java.JavaSparkContext",
                              sparkContext)

      env$jclass <<- "org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtils"
    },

    finalize = function() {
      rm(list = ls(envir = env), envir = env)
    },

    stringDataFrameToVectorDataFrame = function(df) {
      '\\tabular{ll}{
         Description:\\tab \\cr
         \\tab convert the string dataframe the mllib.Vector dataframe. The supported formats are for the following formats
             ((1.2,4.3, 3.4))  or (1.2, 3.4, 2.2) or (1.2 3.4)
             [[1.2,34.3, 1.2, 1.2]] or [1.2, 3.4] or [1.3 1.2]
\\cr
      }'
      stopifnot(class(df) == "SparkDataFrame")
      fname <- "stringDataFrameToVectorDataFrame"
      vdf_jref<-SparkR:::callJStatic(env$jclass, fname, env$sparkContext, df@sdf)
      vdf <- new ("SparkDataFrame", vdf_jref, FALSE)
      vdf
    },

    vectorDataFrameToBinaryBlock = function(df, mc, colname, id = FALSE) {
      '\\tabular{ll}{
      Description:\\tab \\cr
      \\tab convert the mllib.Vector dataframe to systemML binary block.\\cr
      }'
      stopifnot(class(df) == "SparkDataFrame",
                class(mc) == "sysml.MatrixCharacteristics",
                class(colname) == "character")

      fname <- "vectorDataFrameToBinaryBlock"
      vdf_jref<-SparkR:::callJStatic(env$jclass, fname, env$sparkContext, df@sdf, mc$env$jref, id, colname)
      vdf <- SparkR:::RDD(vdf_jref)
      vdf
    },

    dataFrameToBinaryBlock = function(df, mc, id = FALSE, isVector = FALSE) {
      '\\tabular{ll}{
        Description:\\tab \\cr
        \\tab convert the spark dataframe to systemML binary block.\\cr
      }'
      # args checking
      stopifnot(class(df) == "r4ml.matrix",
                class(mc) == "sysml.MatrixCharacteristics")
      fname <- "dataFrameToBinaryBlock"
      bin_block_rdd_jref<-SparkR:::callJStatic(env$jclass, fname, env$javaSparkContext, df@sdf, mc$env$jref, id, isVector)
      return(bin_block_rdd_jref)
    }
  )
)

#' @title An interface to execute dml via the r4ml.matrix and SystemML
#' @description execute the dml code or script via the systemML library
#' @name sysml.execute
#' @param dml a string containing dml code or the file containing dml code
#' @param ... arguments to be passed to the DML
#' @return a named list containing r4ml.matrix for each output
#' @export
#' @examples \dontrun{
#'
#'}
#'
sysml.execute <- function(dml, ...) {
  log_source <- "sysml.execute"
  
  if (missing(dml)) {
    r4ml.err(log_source, "Must have the dml file or script")
  }
  # extract the DML code to be run
  # note dml can be file or string.
  # we check if it ends with .dml and doesn't contains new line then it must be
  # a dml script else it is a string
  is.file <- FALSE
  dml_code <- dml
  if (regexpr('\\.dml$', dml) > 0 && regexpr('\\n', dml)) {
    if (file.exists(dml)) {
      is.file <- TRUE
    } else {
      dml = file.path(r4ml.env$SYSML_ALGO_ROOT(), dml)
      if (file.exists(dml)) {
        is.file <- TRUE
      } else {
        r4ml.err(log_source, dml %++% " file doesn't exists")
      }
    }
  }

  # extract the arguement which are non r4ml.frame
  ml_ctx = sysml.MLContext$new()
  ml_ctx$reset()
  dml_arg_keys <- c()
  dml_arg_vals <- c()
  out_args <- c()
  if (!missing(...)) {
    args <- list(...)
    rdd_utils<- R4ML:::sysml.RDDConverterUtils$new()
    arg_names <- names(args)
    i = 1
    for (arg_val in args) {
      arg_name <- arg_names[i]
      i <- i + 1
      if (class(arg_val) == "r4ml.matrix") {
        r4mlMat = arg_val
        mc <- sysml.MatrixCharacteristics()
        r4mlMat_jrdd <- rdd_utils$dataFrameToBinaryBlock(r4mlMat, mc)
        ml_ctx$registerInput(arg_name, r4mlMat_jrdd, mc)
      } else if (is.null(arg_name) || arg_name == "") {
        ml_ctx$registerOutput(arg_val)
        out_args <- c(out_args, arg_val)
      } else {
        # it is the normal argument and pass in as the parameter to the dml
        dml_arg_keys <- c(dml_arg_keys, as.character(arg_name))
        dml_arg_vals <- c(dml_arg_vals, as.character(arg_val))
      }
    }
  }
  
  dml_result <- tryCatch({
     previous_warning_length <- options()$warning.length
     options(warning.length = 8170)
     sysml_outs <- if (is.file) {
       ml_ctx$execute(dml_code, dml_arg_keys, dml_arg_vals)
     } else {
       ml_ctx$executeScript(dml_code, dml_arg_keys, dml_arg_vals)
     }
   }, warning = function(war) {
        r4ml.err(log_source, paste("DML returned warning:", war))
   }, error = function(err) {
        r4ml.err(log_source, paste("DML returned error:", err))
   }, finally = {
        options(warning.length = previous_warning_length)
   }
  )

  
  # get the output and returns
  outputs <- list()
  for (out_arg in out_args) {
    out_df <- sysml_outs$getDF(out_arg)
    out_r4mlMat <- as.r4ml.matrix(as.r4ml.frame(out_df, repartition = FALSE))
    outputs[out_arg] <- out_r4mlMat
  }
  outputs
}
