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

#' @export
sysml.Script <- setRefClass("sysml.Script",
  fields = list(env = "environment"),
  methods = list(
    initialize = function(jref, ...) {
      env <<- new.env()
      env$jref <<- jref
    },
    
    finalize = function() { # cleanup done by Factory or the systemML 
      SparkR:::cleanup.jobj(env$jref)
    },
    
    
    input = function(df_name, df) {
      logSource <- "sysml.Script.input"
      cls <- as.vector(class(df))
      sdf <- NULL
      df_val <- NA
      if  (cls == 'r4ml.matrix' || cls == 'r4ml.frame') {
        sdf <- SparkR:::dataFrame(df@sdf, df@env$isCached)
        df_val <- sdf@sdf
      } else if (cls == 'SparkDataFrame') {
        sdf <- df@sdf
      } else if (cls == 'jobj') {
        stop("Script.in jobj is not supported")
      } else if (cls == 'numeric') {
        df_val <- df
      } else if (cls == 'character') {
        df_val <- df
      } else if (cls == 'logical') {
        df_val <- df
      } else if (cls == 'integer') {
        df_val <- df
      } else {
        r4ml.err(logSource, "unsupported argument type " %++% cls)
      }
     
      SparkR::sparkR.callJMethod(env$jref, "in", df_name, df_val)
      
      return(.self)
    },
    
    output = function(df_name) {
      SparkR::sparkR.callJMethod(env$jref, "out", df_name)
      return(.self)
    }
                
  )
)

#' @export
sysml.ScriptFactory <- setRefClass("sysml.ScriptFactory",
  fields = list(env = "environment"),
  methods = list(
    initialize = function() {
      env <<- new.env()
      env$jref <<- SparkR:::newJObject("org.apache.sysml.api.mlcontext.ScriptFactory")
    },
    finalize = function() {
      SparkR:::cleanup.jobj(env$jref)
      
    },
    
    dmlFromString = function(dml) {
      script_jref <- SparkR::sparkR.callJMethod(.self$env$jref, "dmlFromString", dml)
      return(sysml.Script$new(jref = script_jref))
    },
    
    dmlFromLocalFile = function(dmlFile) {
      script_jref <- SparkR::sparkR.callJMethod(.self$env$jref, "dmlFromFile", dmlFile)
     return(sysml.Script$new(jref = script_jref))
    }
  )
)

#' @export
sysml.MatrixFormat <- setRefClass("sysml.MatrixFormat",
  fields = list(env = "environment"),
  methods = list(
    initialize = function(format) { # @TODO to see how we can pass in the null
      env <<- new.env()
      validFormats <- c("CSV", "IJV", "DF_DOUBLES_WITH_INDEX", "DF_DOUBLES", "DF_VECTOR_WITH_INDEX", "DF_VECTOR")
      if (!format %in% validFormats) {
        stop("must have one of the following valid matrix formats: ", paste(validFormats, collapse=','))
      }
      .self$env$jref <<- SparkR:::callJStatic("org.apache.sysml.api.mlcontext.MatrixFormat", "valueOf", format)
    },
  
    finalize = function() {
      SparkR:::cleanup.jobj(env$jref) 
    }
    
 )
)
          
#' @export                        
sysml.MatrixMetaData <- setRefClass("sysml.MatrixMetaData",
  fields = list(env = "environment"),
  methods = list(
    initialize = function(format = "CSV", numRows = -1, numColumns = -1) { # @TODO to see how we can pass in the num Rows and numCols and num_nnz
      env <<- new.env()
      formatInst <- sysml.MatrixFormat$new(format = format)
      env$jref <<- SparkR:::newJObject("org.apache.sysml.api.mlcontext.MatrixMetadata", formatInst$env$jref) # passing other args doesn't work
    },
    
    finalize = function() {
      SparkR:::cleanup.jobj(env$jref)
    }
    
  )
)

#' @export
sysml.Matrix <- setRefClass("sysml.Matrix",
  fields = list(env = "environment"),
  methods = list(
    initialize = function(name, jref = NA, ...) {
      env <<- new.env()
      env$name <<- name
      env$jref <<- jref
    },
  
    finalize = function() { # cleanup done by Factory or the systemML 
      SparkR:::cleanup.jobj(env$jref)
    },
  
    
    getSparkDataFrame = function(drop.id = TRUE) {
      sdf <- .self$getDF(drop.id)
      #sdf <- SparkR:::dataFrame(sdf_jref, FALSE)
      return(sdf)
    },
  
    getDF = function(drop.id = TRUE) {
      '\\tabular{ll}{
      Description:\\tab \\cr
      \\tab get the sparkR dataframe from MLOutput \\cr
      \\tab drop.id (default = TRUE) whether to drop internal columns ID\\cr
    }'
      
      colname <- .self$env$name
      
      df_jref <- SparkR::sparkR.callJMethod(.self$env$jref, "toDF")
      
      # df_unsorted <- new("SparkDataFrame", sdf=df_jref, isCached=FALSE)# ALOK TODO rm 
      df_unsorted <-  SparkR:::dataFrame(df_jref, FALSE) # ALOK TODO use this
      
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

#' @export
sysml.MLResults <- setRefClass("sysml.MLResults",
  fields = list(env = "environment"),
  methods = list(
    initialize = function(jref, ...) {
      env <<- new.env()
      env$jref <<- jref
    },
    
    finalize = function() { # cleanup done by Factory or the systemML 
      SparkR:::cleanup.jobj(env$jref)
    },
    
    getMatrix = function(oname) {
      stopifnot(class(oname) == "character")
      mat_jref <- SparkR::sparkR.callJMethod(env$jref, "getMatrix", oname)
      sysml.mat <- sysml.Matrix$new(oname, mat_jref)
      return(sysml.mat)
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
      env$jref <<- SparkR:::newJObject("org.apache.sysml.api.mlcontext.MLContext", sparkContext)
      env$jref_scriptExecutor <<- SparkR:::newJObject("org.apache.sysml.api.mlcontext.ScriptExecutor") # ALOK TODO rm
      env$jref_scriptFactory <<- SparkR:::newJObject("org.apache.sysml.api.mlcontext.ScriptFactory") # ALOK TODO rm
    },

    finalize = function() {
      SparkR:::cleanup.jobj(env$jref)
      SparkR:::cleanup.jobj(env$jref_scriptExecutor)
      SparkR:::cleanup.jobj(env$jref_scriptFactory) 
    },

    reset = function() {
      '\\tabular{ll}{
         Description:\\tab \\cr
           \\tab reset the MLContext \\cr
       }'
      SparkR::sparkR.callJMethod(env$jref, "reset")
    },

 

    executeScriptBase = function(dml_script) {
      '\\tabular{ll}{
      Description:\\tab \\cr
      \\tab execute the string containing the dml code
      }
      \\tabular{lll}{
      Arguments:\\tab \\tab \\cr
      \\tab dml_script \\tab string containing the dml script whose variables has been bound using registerInput and registerOut \\cr
      }
      '
      
      stopifnot(class(dml_script) == "sysml.Script")

   
      out_jref <- NULL
      
      previous_log_level <- jlogger$getLevel()
      invisible(jlogger$setLevel(r4ml.env$SYSML_LOG_LEVEL))
      
      dml_script_jref <- dml_script$env$jref
      
      out_jref <- SparkR::sparkR.callJMethod(
        env$jref, "execute",
        dml_script_jref
      )
      
      
      invisible(jlogger$setLevel(previous_log_level))
      
      #@TODO. get sysmlSqlContext from the ctor
      outputs <- sysml.MLResults$new(out_jref)
    },

    
    execute = function(script) {
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
      .self$executeScriptBase(script)
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
  
  scriptFactory <- sysml.ScriptFactory$new()
  script <- NA
  if (is.file) {
    script <- scriptFactory$dmlFromLocalFile(dml)
  } else {
    script <- scriptFactory$dmlFromString(dml)
  }
  
  dml_arg_keys <- c()
  dml_arg_vals <- c()
  out_args <- c()
  in_args <- c()
  if (!missing(...)) {
    args <- list(...)
    arg_names <- names(args)
    i = 1
    for (arg_val in args) {
      arg_name <- arg_names[i]
      i <- i + 1
      if (class(arg_val) == "r4ml.matrix") {
        r4mlMat = arg_val
        md <- sysml.MatrixMetaData$new()
        script$input(arg_name, arg_val) # TODO add the matrix metadata later
        in_args <- c(in_args, arg_name)
      } else if (is.null(arg_name) || arg_name == "") {
        script$output(arg_val)
        out_args <- c(out_args, arg_val)
      } else {
        script$input(arg_name, arg_val)
        # it is the normal argument and pass in as the parameter to the dml
        dml_arg_keys <- c(dml_arg_keys, as.character(arg_name))
        dml_arg_vals <- c(dml_arg_vals, as.character(arg_val))
        
      }
    }
  }
  
  dml_result <- tryCatch({
     previous_warning_length <- options()$warning.length
     options(warning.length = 8170)
     sysml_outs <- ml_ctx$execute(script);
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
    out_mat <- sysml_outs$getMatrix(out_arg)
    out_sdf <- out_mat$getSparkDataFrame(out_mat)
    out_r4mlMat <- as.r4ml.matrix(as.r4ml.frame(out_sdf, repartition = FALSE))
    outputs[out_arg] <- out_r4mlMat
  }
  outputs
}
