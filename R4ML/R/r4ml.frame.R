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
#' @include hydrar.vector.R
requireNamespace("SparkR")
#'
#' hydrar.frame, An S4 class which is inherited from SparkR SparkDataFrame
#'
#' Ideally one shouldn't be calling it constructor and use as.hydrar.frame
#' in case one has to call this see the examples
#'
#' @name hydrar.frame
#' @slot same as SparkR::SparkDataFrame
#'
#' @examples \dontrun{
#'
#'  spark_df <- SparkR::createDataFrame(iris)
#'  hydrar_frame <- new("hydrar.frame", sdf=spark_df@@sdf, isCached=spark_df@@env$isCached)
#'
#' }
#'
#' @export
#'
#' @seealso \link{as.hydrar.frame} and SparkR::SparkDataFrame
setClass("hydrar.frame", contains="SparkDataFrame")

#' Check if hydrar.frame is numeric or not
#'
#' This method can be used for working with various matrix utils which requires
#' input hydrar.frame to be numeric
#'
#' @name is.hydrar.numeric
#' @param object a hydrar.frame
#' @param ... future optional additional arguments to be passed to or from methods
#' @return TRUE if all the cols are numeric else FALSE
#' @export
#'
#' @examples \dontrun{
#'
#'    data <- as.hydrar.frame(as.data.frame(iris))
#'    is.hydrar.numeric(data) #This is FALSE
#'    hf2=as.hydrar.frame(iris[c("Sepal.Length", "Sepal.Width")])
#'    is.hydrar.numeric(hf2) # This is TRUE
#'
#'}
#'
setGeneric("is.hydrar.numeric", function(object, ...) {
  standardGeneric("is.hydrar.numeric")
})

setMethod("is.hydrar.numeric",
  signature(object = "hydrar.frame"),
  function(object, ...) {
    cnames <- SparkR::colnames(object)
    ctypes <- SparkR::coltypes(object)
    bad_cols <- cnames[which(sapply(ctypes, function(e) !e %in% c("numeric", "integer", "double")))]
    if (length(bad_cols) >= 1) {
      return (FALSE)
    }
    return (TRUE)
  }
)

hydrar.calc.num.partitions <- function(object_size) {
  logSource <- "calc num partitions"

  # attempt to detect the # of CPU cores on machine
  cores <- parallel::detectCores(all.tests = TRUE)
  
  if (is.null(cores) | cores < 1) {
    hydrar.warn(logSource, "unable to detect number of CPU cores")
    cores <- 8 # default to 8 cores
  }

  num_partitions <- as.numeric(object_size) / hydrar.env$MIN_PARTITION_SIZE
  num_partitions <- ceiling(num_partitions)
  
  if (num_partitions < cores) {
    # we should have at least as many partitions as cpu cores
    num_partitions <- cores
  }
  
  hydrar.info(logSource, paste(num_partitions, "partitions"))
  
  return(num_partitions)
}


#' Coerce to a HydraR Frame
#'
#' Convert a data.frame, hydrar.matrix, or Spark DataFrame into a hydrar.frame
#'
#' @name as.hydrar.frame
#' @param object a R data.frame or SparkDataFrame
#' @param repartition (data.frame only) (logical) should the data automatically
#' be repartitioned
#' @param numPartitions (data.frame only) (numeric) number of partitions
#' @param ... future optional additional arguments to be passed to or from methods
#' @return a hydrar.frame
#' @export
#' @examples \dontrun{
#'    hf1 <- as.hydrar.frame(iris)
#'    hf2 <- as.hydrar.frame(SparkR::createDataFrame(iris))
#' }
setGeneric("as.hydrar.frame", function(object, repartition = TRUE,
                                       numPartitions = NA, ...) {
  standardGeneric("as.hydrar.frame")
})

setMethod("as.hydrar.frame",
  signature(object = "SparkDataFrame"),
  function(object, ...) {
    logSource <- "as.hydrar.frame"
    
    if (!hydrar.env$HYDRAR_SESSION_EXISTS) {
      hydrar.err(logSource,
                 'No HydraR session exists (call "hydrar.session()")')
    }

    hydra_frame <- new("hydrar.frame", sdf=object@sdf, isCached=object@env$isCached)
    hydra_frame

  }
)

setMethod("as.hydrar.frame",
  signature(object = "data.frame"),
  function(object, repartition = TRUE, numPartitions = NA, ...) {
    logSource <- "as.hydrar.frame"

    if (!hydrar.env$HYDRAR_SESSION_EXISTS) {
      hydrar.err(logSource,
                 'No HydraR session exists (call "hydrar.session()")')
    }
    
    # need to get the object size before we make it a Spark DataFrame
    object_size <- object.size(object)
    if (object_size < hydrar.env$MIN_REPARTION_SIZE) {
      # don't repartion small objects
      # this is required for some test cases (repartioning re-orders rows) and
      # likely results in better performance with small datasets
      repartition <- FALSE
    }

    spark_df <- SparkR::createDataFrame(object)

    if (repartition) {

      if (is.na(numPartitions)) {
        numPartitions <- hydrar.calc.num.partitions(object_size)
      }

      hydrar.info(logSource,
                  paste("repartitioning an object of size:", object_size, 
                        "into", numPartitions, "partitions"))

      spark_df <- SparkR::repartition(spark_df, numPartitions = numPartitions)
    }
    
    data <- as.hydrar.frame(spark_df, ...)
    
    return(data)
  }
)

setMethod(f = "show", signature = "hydrar.frame", definition = 
  function(object) {
    logSource <- "hydrar.frame.show"
    # Get the query result as a data.frame
    df <- SparkR::as.data.frame(
      if (hydrar.env$DEFAULT_SHOW_ROWS > 0) {
        SparkR::head(object, hydrar.env$DEFAULT_SHOW_ROWS)
      } else {
        object                
      }
    )

    if (.hydrar.isNullOrEmpty(df)) {
      df <- data.frame()
    }            
    if (ncol(object) == 0) {
      cat("hydrar.frame with 0 columns\n")
    } else if (nrow(df) == 0) {
      cat(paste(colnames(object), collapse="    "))
      cat("\n<0 rows>\n")
    } else {
      # Show the contents of the hydra.frame as a data.frame
      show(df)
      cat("... " %++% " showing first " %++% hydrar.env$DEFAULT_SHOW_ROWS %++% " rows only.\n")
    }
    invisible(NULL);
  }
)

#' @name hydrar.impute
#' @title Missing Value Imputation
#' @export
#' @description Imputes a missing value with either the mean of the feature or a user supplied constant.
#' @details List parameter takes a named list with columns to impute for as the names and either "mean", empty (in which case mean will be assumed), or a constant to impute as the values
#' 
#' @param data (hydrar.frame) A hydrar.frame
#' @param ... (list) A named list with names that represent the columns to be fitted, values are imputed.
#' 
#'@examples \dontrun{
#'  # Load Dataset
#'  df <- as.DataFrame(airquality)
#'  head(df)
#'  
#'  df <- as.hydrar.frame(df)
#'  
#'  # Example with "mean" value in list.
#'  new_df <- hydrar.impute(df, list("Ozone"="mean"))
#'  head(new_df$data)
#'  
#'  # Example with no arguments - mean imputation is used as the default
#'  new_df <- hydrar.impute(df, list("Ozone", "Solar_R"))
#'  head(new_df$data)
#'  
#'  # Example of constant imputation.
#'  new_df <- hydrar.impute(df, list("Ozone"=4000, "Solar_R"=-5))
#'  head(new_df$data)
#'  
#'  # Constant and mean imputation can be combined.
#'  new_df <- hydrar.impute(df, list("Ozone"=4000, "Solar_R"="mean"))
#'  head(new_df$data)
#'}
# NOTE: add the more specific arguement to ...
# NOTE: output must contain at least same or more columns as input
setGeneric("hydrar.impute", function(data, ...) {
  standardGeneric("hydrar.impute")
})

setMethod("hydrar.impute",
  signature(data = "hydrar.frame"),
  function(data, df_columns){
    logSource <- "hydrar.impute"
    strings <- list()
    df_columns <- as.list(df_columns)
    name <- names(df_columns)
    column_names <- list()
    constants <- list()
    constant_names <- list()
    
    for(col in names(df_columns)){
      if(!is.hydrar.numeric(as.hydrar.frame(SparkR::select(data, col)))){
        hydrar.err(logSource, "Column for imputation must be numeric")
      }
    }
    
    # If no names in list, default to mean imputation
    if (is.null(name)){
      column_names <- df_columns
      for(i in df_columns){
        strings <- c(strings, (paste0("SparkR::mean(data$", i, ")")))
      }
      # If there are names in named list, parse list to determine mean vs. constant as well as column names
    } else {
      for(i in 1:length(df_columns)){
        # If the name is empty, use the mean
        if(name[i] == "" ){
          column_names <- c(column_names, df_columns[i])
          strings <- c(strings, (paste0("SparkR::mean(data$", name[i], ")")))
          # If the list element is "mean", use mean imputation
        } else if(df_columns[i] == "mean") {
          column_names <- c(column_names, name[i])
          strings <- c(strings, (paste0("SparkR::mean(data$", name[i], ")")))
          # If the list element is a constant, don't make call to mean
        } else if (as.logical(lapply(df_columns[i], is.numeric))[1]){
          constant_names <- c(constant_names, name[i])
          constants <- c(constants, (df_columns[i]))
          # Default to mean imputation otherwise
        } else {
          column_names <- c(column_names, i)
          strings <- c(strings, (paste0("SparkR::mean(data$", name[i], ")")))
        } 
      }
    }
    
    values <- list()
    # If there are means to collect from workers, parallelize mean calls with aggregate
    if(length(strings) > 0){
      for_call <- list(data)
      for (i in strings){
        # Use eval to convert the strings into function calls
        for_call <- c(for_call, SparkR::column(eval(parse(text=i))@jc))
      }
      values <- as.list(SparkR::collect(do.call(SparkR::agg, for_call)))
      names(values) <- column_names
    }
    names(constants) <- constant_names
    
    # Combine the constants and means into a single list
    values <- as.list(c(values, constants))
    
    # Convert values list to metadata
    metadata <- list2env(values, parent=emptyenv())
    return_df <- as.hydrar.frame(SparkR::fillna(x = data, value = values))
    
    return(list(data=return_df, metadata=metadata))
  }
)


#' @name hydrar.recode
#' @title Recode the categorical value into the nominal values
#' @description Specified categorical columns will be 
#'  mapped into consecutive numeric categories. For example, if a column has 
#'  values "Low", "Medium", and "High", these will be mapped to 1, 2, and 3. 
#'  \strong{Note}: All columns of type character will be automatically recoded.
#'  The order of the recoded values is non-deterministic.
#' @param data a hydrar.frame
#' @param ... list of columns to be recoded. If no columns are given all 
#'      the columns are recoded
#' @details The transformed dataset will be returned as a \code{hydrar.frame}
#'  object. The transform meta-info is also returned. This is helpful to keep
#'  track of which transformations were performed as well as to apply the same
#'  set of transformations to a different dataset.The structure of the metadata
#'  is the nested env
#'    NOTE: output contain at least same number of columns as the original hydrar.frame
#' @export
#'      
#' @examples \dontrun{
#'  data <- as.hydrar.frame(iris)
#'  hf_rec <- hydrar.recode(data, c("Species"))
#'
#'  # make sure that recoded value is right
#'  rhf_rec <- SparkR::as.data.frame(hf_rec$data)
#'  rhf_data <- rhf_rec # recoded hydrar.frame
#'  rhf_md <- rhf_rec$metadata # metadata associated with the recode
#'  show(rhf_data)
#'  rhf_md$Species$setosa # check one of the recoded value
#' }
#'
setGeneric("hydrar.recode", function(data, ...) {
  standardGeneric("hydrar.recode")
})


setMethod("hydrar.recode",
  signature(data = "hydrar.frame"),
  function(data, ...) {
    logSource <- "hydrar.recode"

    # get the list of all input columns and set default (if needed)
    icols <- unlist(list(...), recursive = TRUE)
    hf_colnames <- SparkR::colnames(data)
    if (missing(icols) || length(icols) == 0) {
      icols <- hf_colnames
    }
    nurow_max <- 1e6 # maximum number of unique element

    # dynamically create command to be executed later

    #salt <- "r:"
    salt <- ""
    empty_string_recode = hydrar.env$EMPTY_STRING_RECODE
    # create the env aka hashmap for each column
    icol2rec_env = new.env(hash=TRUE, parent = emptyenv())
    for (icol in icols) {
      hydrar.debug(logSource, paste("on column", icol))
      icol_df <- SparkR::select(data, icol)
      uicol_df <- SparkR::distinct(icol_df)
      uicol_nr <- SparkR::nrow(uicol_df)
      if (uicol_nr > nurow_max) {
        hydrar.err(logSource, "Number of unique element in the col "
                   %++% icol %++% "exceed maximum" %++% nurow_max)
      }
      uicol_rdf_tmp <- SparkR::as.data.frame(uicol_df)
      # since empty string can't be the key to the env
      uicol_rdf_tmp[uicol_rdf_tmp==''] <- empty_string_recode
      
      #make sure that we have defined order of the distinct i.e natural order.
      #note that distinct can give different order in sep run
      uicol_rdf <- setNames(
        SparkR::as.data.frame(uicol_rdf_tmp[order(uicol_rdf_tmp[icol]),]),
        icol)

      # this is the recode mapping for column icol, which will be used later
      # and also will be used as metadata
      # since some of the columns will be "" so we use this hack
      icol2recode <- list2env(as.list(
        setNames(1:uicol_nr, paste(salt, uicol_rdf[,icol], sep=""))),
        parent=emptyenv()
      )
      assign(icol, icol2recode, envir=icol2rec_env)
    }

    hf_colid2name <- list2env(
      as.list(setNames(hf_colnames, 1:length(hf_colnames))),
      parent=emptyenv()
    )

    hf_colname2id <- list2env(
      as.list(setNames(1:length(hf_colnames), hf_colnames)),
      parent=emptyenv()
    )

    # create the new RDD of the recoded columns, note that all the recoding
    # is done in single pass
    new_row_rdd <- SparkR:::lapply(
      data,
      function(row) {
        ret = list()
        for (i in 1:length(hf_colnames)) {
          row_i <- row[[i]]
          cname <- get(toString(i), envir=hf_colid2name, inherits = F)
          if (exists(cname, envir=icol2rec_env, inherits = F)) {
            icol2recode <- get(cname, envir=icol2rec_env, inherits = F)
            if (exists(cname, envir=icol2rec_env, inherits = F)) {
              if (row_i == '') {
                row_i = empty_string_recode
              }
              row_i_salty <- paste(salt, row_i, sep="")
              
              rec_val <- get(row_i_salty, icol2recode, inherits = F)
              ret = c(ret, rec_val)
            } else {
              hydrar.err(logSource, "can't find the recode value")
            }
          } else {
            ret = c(ret, row_i)
          }
        }
        as.list(ret)
      }
    )
   
    #calculate the new schema
    old_sch <- SparkR::schema(data)
    old_sch_flds <- old_sch$fields()
    new_sf = list()
    for (i in 1:length(hf_colnames)) {
      old_sch_fld <- old_sch_flds[[i]]
      cname <- get(toString(i), envir=hf_colid2name, inherits = F)
      new_sch_fld <- old_sch_fld
      if (exists(cname, envir=icol2rec_env, inherits = F)) {
        old_sch_fld <- old_sch_flds[[i]]
        new_sch_fld <- SparkR::structField(old_sch_fld$name(), "integer",
                                   old_sch_fld$nullable())
      } else {
        #default  new_sch_fld <- old_sch_fld
      }
      new_sf[[length(new_sf)+1]] <- new_sch_fld
    }
    new_row_rdd_sch <- do.call(SparkR::structType, as.list(new_sf))
    res_hf <- as.hydrar.frame(SparkR::as.DataFrame(new_row_rdd,
                                                   schema = new_row_rdd_sch))
    meta_db <- icol2rec_env
    list(data=res_hf, metadata=meta_db)
  }
)

#' @name hydrar.normalize
#' @title Normalize the scale value by shifting and scaling
#' @description Specified scale columns will be 
#'  shifting by mean and divided by it's sample standard deviation. In case, 
#'  we do only the shifting by mean. 
#' @param data a hydrar frame
#' @param ... list of columns to be normalized. If no columns are given all 
#'      the columns are recoded
#' @details The transformed dataset will be returned as a \code{hydrar.frame}
#'  object. The transform meta-info is also returned. This is helpful to keep
#'  track of which transformations were performed as well as to apply the same
#'  set of transformations to a different dataset.The structure of the metadata
#'  is the nested env
#' @export
#'      
#' @examples \dontrun{
#'  data <- as.hydrar.frame(as.data.frame(iris))
#'  data_norm_info <- hydrar.normalize(data, c("Sepal_Width", "Petal_Length"))
#'
#'  # make sure that recoded value is right
#'  data_norm <- data_norm_info$data
#'  data_md <- data_norm_info$metadata # metadata associated with the normalization
#'  show(data_norm)
#'  ls.str(data_md) # check the metadata corresponding to norm ops
#' }
#'
setGeneric("hydrar.normalize", function(data, ...) {
  standardGeneric("hydrar.normalize")
})

setMethod("hydrar.normalize",
  signature(data = "hydrar.frame"),
  function(data, ...) {
    logSource <- "hydrar.normalize"
    hfnames <- SparkR::colnames(data)
    hftypes <- SparkR::coltypes(data)
    
    args <- list(...)
    if (length(args) == 0) {
      args <- hfnames
    }
    
    inames <- args
    if (length(args) == 1 && class(args[1]) == "list") {
      inames <- args[[1]]
    }
    
    # check that all inputs to be imputed is in the class
    uinames <- inames[which(is.na(match(inames, hfnames)))]
    if (length(uinames) != 0) {
      hydrar.err(logSource, paste(uinames, "columns not found in the input data"))
    }
    
    itypes <- hftypes[match(inames, hfnames)]
    
    
    # check that the data types of the imputed cols are of numeric
    # we have the constant string so take care of it
    binames <- inames[which(sapply(itypes, function(e) !e %in% c("numeric", "integer", "double")))]
    if (length(binames) >= 1) {
      hydrar.err(logSource, paste(binames, " input columns are not numeric and can't be imputed"))
    }
    
    # dynamically create command to be executed later
    rstr <- "SparkR::agg(data"
    for (iname in inames) {
      
      mean_str <- paste("SparkR::", "mean" , "(as.sparkr.column(data$", iname, "))", sep="")
      ndfname <- paste("mean_", iname, sep="")
      rstr <- paste(rstr, ", ", ndfname, " = ", mean_str, sep="")
      
      sd_str <- paste("SparkR::", "sd" , "(as.sparkr.column(data$", iname, "))", sep="")
      ndfname <- paste("stddev_", iname, sep="")
      rstr <- paste(rstr, ", ", ndfname, " = ", sd_str, sep="")
      
    }
    
    rstr = paste(rstr, ")", sep="")
    
    
    # calc the mean of all the columns
    rhfstats <- eval(parse(text=rstr))
    hfstats <- SparkR::as.data.frame(rhfstats)
    
    mstr <- "SparkR::mutate(data"
    for (iname in inames) {
      new_col <- "new_" %++% iname
      mean <- hfstats[['mean_' %++% iname]]
      sd <- hfstats[['stddev_' %++% iname]]
      if (sd == 0.0) {
        # meaning that all the values are equal and hence substracting
        # it with zero will give 0 vector and div by 0 will give inf. so we will not
        # divide by zero. instead have the default 1.0
        sd <- 1
      }
      mstr <- mstr %++% sprintf(", %s = (as.sparkr.column(data$%s)-%s)/(2*%s)", new_col, iname, mean, sd)
    }
    mstr <- mstr %++% ")"
    
    mhf <- eval(parse(text=mstr))
    lu=setNames(sapply(inames, function (e) "new_" %++% e), inames)
    new_cols <- sapply(hfnames,
                       function(e) ifelse(e %in% inames, lu[[e]], e) )
    new_df <- SparkR::select(mhf, new_cols)
    new_hf <- as.hydrar.frame(new_df)
    SparkR::colnames(new_hf) <- hfnames

    # now create the metadata
    metadata <- new.env(parent=emptyenv())
    for (iname in inames) {
      
      mean <- hfstats[['mean_' %++% iname]]
      sd <- hfstats[['stddev_' %++% iname]]
      if (sd == 0.0) {
        # meaning that all the values are equal and hence substracting
        # it with zero will give 0 vector and div by 0 will give inf. so we will not
        # divide by zero. instead have the default 1.0
        sd <- 1
      }
      col_info <- list("mean" = mean, "stddev" = sd)
      assign(iname, col_info, metadata)
    }
    list(data=as.hydrar.frame(new_hf), metadata=metadata)
  }
)          

#' @name hydrar.binning
#' @title Binning
#' @export
#' @description Takes a column and a number of bins and returns a new column with the average value of the bin each value has been placed into.
#' @param data (hydrar.frame) The hydrar.frame to bin columns for.
#' @param columns (list) List of column names to create bins with.
#' @param number (numeric) Number of bins to create.
#' 
#' @examples \dontrun{
#' # Setup Data
#' df <- iris
#' df$Species <- (as.numeric(df$Species))
#' iris_df <- as.hydrar.frame(df)
#' 
#' binned_df = hydrar.binning(iris_df, "Sepal_Width", 20)
#' head(binned_df$data)
#' }
#'
# NOTE: add the more specific arguement to ...
# NOTE: output must contain at least same or more columns as input
setGeneric("hydrar.binning", function(data, ...) {
  standardGeneric("hydrar.binning")
})

setMethod("hydrar.binning",
  signature(data = "hydrar.frame"),
  function(data, columns, number){
    metadata <- new.env(parent=emptyenv())
    for(name in as.list(columns)){
      column <- data[[name]]
      
      if(!is.hydrar.numeric(as.hydrar.frame(SparkR::select(data, name)))){
        hydrar.err(logSource, "Must provide numeric columns.")
      }
      
      icolumn <- column
      # Convert for hydrar vector to SparkR column to access aggregation functions
      if (class(column) == "hydrar.vector") {
        icolumn <- SparkR::column(column@jc)
      } else {}
      
      # Grab min/max, collect will be fine since this will only return min max
      minmax <- SparkR::collect(SparkR::agg(data, min(icolumn), max(icolumn)))
      minimum = minmax[1][[1]]
      maximum = minmax[2][[1]]
      range = ((maximum-minimum)/number) 
      
      # We can compute the nth bin of each value by basing list at 0 and dividing by range
      # Add epsilon to avoid new bin for largest value
      # @TODO Find more elegant way to compute this
      int_bins = floor(((icolumn-minimum)/(maximum-minimum+.00001))*number)
      # Re-add the minimum to get the floor of binned values, add range/2 to get average
      avg_bins = ((int_bins*range+minimum) + range/2)
      # Grab initial colnames for eventual rearrange
      hf_colnames <- SparkR::colnames(data)

      # Create original column name
      new_name <- paste0(name, "_new")
      while(new_name %in% hf_colnames){
        new_name <- paste0(new_name, "_new")
      }
      
      # Establish outputs
      data <- SparkR::withColumn(data, new_name, avg_bins)
      # Delete Original Column
      eval(parse(text = paste0("data$", name, " <- NULL")))
      # Rename new column
      data <- SparkR::withColumnRenamed(data, existingCol = new_name, newCol = name)
      
      # Rearrange columns
      data <- as.hydrar.frame(SparkR::select(data, hf_colnames))
      metadata[[name]] = list(featureName=name,
                              minValue=minimum,
                              maxValue=maximum,
                              binWidth=range,
                              numBins=number)
    }
    list(data = data, metadata = metadata)
  }
)
