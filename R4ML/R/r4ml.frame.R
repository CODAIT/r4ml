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
#' hydrar.frame, An S4 class which is inherited from SparkR DataFrame
#'
#' Ideally one shouldn't be calling it constructor and use as.hydrar.frame
#' in case one has to call this see the examples
#'
#' @name hydrar.frame
#' @slot same as SparkR::DataFrame
#'
#' @examples \dontrun{
#'
#'  spark_df <- SparkR::createDataFrame(sqlContext, iris)
#'  hydra_frame <- new("hydrar.frame", sdf=spark_df@@sdf, isCached=spark_df@@env$isCached)
#'
#' }
#'
#' @export
#'
#' @seealso \link{as.hydrar.frame} and SparkR::DataFrame
setClass("hydrar.frame", contains="DataFrame")

#' Check if hydrar.frame is numeric or not
#'
#' This method can be used for working with various matrix utils which requires
#' input hydrar.frame to be numeric
#'
#' @name is.hydrar.numeric
#' @param object a hydrar.frame
#' @return TRUE if all the cols are numeric else FALSE
#' @export
#'
#' @examples \dontrun{
#'
#'    hf <- as.hydrar.frame(as.data.frame(iris))
#'    is.hydrar.numeric(hf) #This is FALSE
#'    hf2=as.hydrar.frame(iris[c("Sepal.Length", "Sepal.Width")])
#'    is.hydrar.numeric(hf2) # This is TRUE
#'
#'}
#'
setGeneric("is.hydrar.numeric", function(object, ...) {
  standardGeneric("is.hydrar.numeric")
})

#' @export
setMethod("is.hydrar.numeric",
  signature(object = "hydrar.frame"),
  function(object, ...) {
    cnames <- SparkR:::colnames(object)
    ctypes <- SparkR:::coltypes(object)
    bad_cols <- cnames[which(sapply(ctypes, function(e) !e %in% c("numeric", "integer", "double")))]
    if (length(bad_cols) >= 1) {
      return (FALSE)
    }
    return (TRUE)
  }
)

#' Convert the various  data.frame into the hydraR data frame.
#'
#' This is the convenient method of converting the data in the distributed hydraR
#'
#' @name as.hydrar.frame
#' @param object a R data.frame or SparkR::DataFrame
#' @return hdf a hydraR dataframe
#' @export
#' @examples \dontrun{
#'    hf1 <- as.hydrar.frame(iris)
#'    hf2 <- as.hydrar.frame(SparkR::createDataFrame(sqlContext, iris))
#' }
setGeneric("as.hydrar.frame", function(object, ...) {
  standardGeneric("as.hydrar.frame")
})

#' @export
setMethod("as.hydrar.frame",
  signature(object = "DataFrame"),
  function(object, ...) {
    hydra_frame <- new("hydrar.frame", sdf=object@sdf, isCached=object@env$isCached)
    hydra_frame
  }
)

#' @export
setMethod("as.hydrar.frame",
  signature(object = "data.frame"),
  function(object, ...) {
    spark_df <- SparkR::createDataFrame(sqlContext, object)
    as.hydrar.frame(spark_df)
  }
)

# 
#' Show the content of the hydrar.frame
#'
#' This is the convenient method for showing the content of the hydrar.frame. The output
#' is similar to the output produce by R
#'
#' @name show
#' @param object a R data.frame 
#' @export
#' @examples \dontrun{
#'    hf1 <- as.hydrar.frame(iris)
#'    show(hf1)
#' }
setMethod(f = "show", signature = "hydrar.frame", definition = 
            function(object) {
              logSource <- "hydrar.frame.show"
              # Get the query result as a data.frame
              df <- as.data.frame(
                if (hydrar.env$DEFAULT_SHOW_ROWS > 0) {
                  SparkR:::head(object, hydrar.env$DEFAULT_SHOW_ROWS)
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

#' @export
# NOTE: add the more specific arguement to ...
# NOTE: output must contain atleast same or more columns as input
setGeneric("hydrar.impute", function(hf, ...) {
  standardGeneric("hydrar.impute")
})

setMethod("hydrar.impute",
  signature(hf = "hydrar.frame"),
  function(hf, ...) {
    res_hf <- hf; # change it in future
    meta_db <- list()
    warning("**WARNING** hydrar.impute is not implemented yet")    
    list(data=res_hf, metadata=meta_db)
  }
)


#' @name hydrar.recode
#' @title Recode the categorical value into the nominal values
#' @description Specified categorical columns will be 
#'  mapped into consecutive numeric categories. For example, if a column has 
#'  values "Low", "Medium", and "High", these will be mapped to 1, 2, and 3. 
#'  \strong{Note}: All columns of type character will be automatically recoded.
#'  The order of the recoded values is non-deterministic. 
#'  @param ... list of columns to be recoded. If no columns are given all 
#'      the columns are recoded
#' @details The transformed dataset will be returned as a \code{hydrar.frame}
#'  object. The transform meta-info is also returned. This is helpful to keep
#'  track of which transformations were performed as well as to apply the same
#'  set of transformations to a different dataset.The structure of the metadata
#'  is the nested env
#'    NOTE: output contain atleast same number of columns as the original hydrar.frame
#' @export
#'      
#' @examples \dontrun{
#'  hf <- as.hydrar.frame(as.data.frame(iris))
#'  hf_rec = hydrar.recode(hf, c("Species"))
#'
#'  # make sure that recoded value is right
#'  rhf_rec <- SparkR:::as.data.frame(hf_rec$data)
#'  rhf_data <- rhf_rec$data # recoded hydrar.frame
#'  rhf_md <- rhf_rec$metadata # metadata associated with the recode
#'  show(rhf_data)
#'  rhf_md$Species$setosa # check one of the recoded value
#' }
#'
setGeneric("hydrar.recode", function(hf, ...) {
  standardGeneric("hydrar.recode")
})


setMethod("hydrar.recode",
  signature(hf = "hydrar.frame"),
  function(hf, ...) {

    # get the list of all input columns and set default (if needed)
    icols <- unlist(list(...), recursive = T)
    hf_colnames <- SparkR:::colnames(hf)
    if (missing(icols) || length(icols) == 0) {
      icols <- hf_colnames
    }
    nurow_max <- 1e6 # maximum number of unique element

    # dynamically create command to be executed later

    #salt <- "r:"
    salt <- ""
    # create the env aka hashmap for each column
    icol2rec_env = new.env(hash=TRUE, parent = emptyenv())
    for (icol in icols) {
      icol_df <- SparkR:::select(hf, icol)
      uicol_df <- SparkR:::distinct(icol_df)
      uicol_nr <- SparkR:::nrow(uicol_df)
      if (uicol_nr > nurow_max) {
        hydrar.err("Number of unique element in the col " %++% icol %++%
                     "exceed maximum" %++% nurow_max)
      }
      uicol_rdf_tmp <- SparkR::as.data.frame(uicol_df)
      #make sure that we have defined order of the distinct i.e natural order.
      #note that distinct can give different order in sep run
      uicol_rdf <- setNames(
        as.data.frame(uicol_rdf_tmp[order(uicol_rdf_tmp[icol]),]),
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
      hf,
      function(row) {
        ret = list()
        for (i in 1:length(hf_colnames)) {
          row_i <- row[[i]]
          cname <- get(toString(i), envir=hf_colid2name, inherits = F)
          if (exists(cname, envir=icol2rec_env, inherits = F)) {
            icol2recode <- get(cname, envir=icol2rec_env, inherits = F)
            if (exists(cname, envir=icol2rec_env, inherits = F)) {
              row_i_salty <- paste(salt, row_i, sep="")
              rec_val <- get(row_i_salty, icol2recode, inherits = F)
              ret = c(ret, rec_val)
            } else {
              stop("hydrar.recode FATAL can't find the recode value")
            }
          } else {
            ret = c(ret, row_i)
          }
        }
        as.list(ret)
      }
    )
   
    #calculate the new schema
    old_sch <- SparkR:::schema(hf)
    old_sch_flds <- old_sch$fields()
    new_sf = list()
    for (i in 1:length(hf_colnames)) {
      old_sch_fld <- old_sch_flds[[i]]
      cname <- get(toString(i), envir=hf_colid2name, inherits = F)
      new_sch_fld <- old_sch_fld
      if (exists(cname, envir=icol2rec_env, inherits = F)) {
        old_sch_fld <- old_sch_flds[[i]]
        new_sch_fld <- structField(old_sch_fld$name(), "integer",
                                   old_sch_fld$nullable())
      } else {
        #default  new_sch_fld <- old_sch_fld
      }
      new_sf[[length(new_sf)+1]] <- new_sch_fld
    }
    new_row_rdd_sch <- do.call("structType", as.list(new_sf))
    res_hf <- as.hydrar.frame(as.DataFrame(sqlContext, new_row_rdd, new_row_rdd_sch))
    meta_db <- icol2rec_env
    list(data=res_hf, metadata=meta_db)
  }
)

#' @name hydrar.normalize
#' @title Normalize the scale value by shifting and scaling
#' @description Specified scale columns will be 
#'  shifting by mean and divided by it's sample standard deviation. In case, 
#'  we do only the shifting by mean. 
#'  @param ... list of columns to be normalized. If no columns are given all 
#'      the columns are recoded
#' @details The transformed dataset will be returned as a \code{hydrar.frame}
#'  object. The transform meta-info is also returned. This is helpful to keep
#'  track of which transformations were performed as well as to apply the same
#'  set of transformations to a different dataset.The structure of the metadata
#'  is the nested env
#' @export
#'      
#' @examples \dontrun{
#'  hf <- as.hydrar.frame(as.data.frame(iris))
#'  hf_norm_info = hydrar.normalize(hf, c("Sepal_Width", "Petal_Length"))
#'
#'  # make sure that recoded value is right
#'  hf_norm <- hf_norm_info$data
#'  hf_md <- hf_norm_info$metadata # metadata associated with the normalization
#'  show(hf_norm)
#'  ls.str(hf_md) # check the metadata corresponding to norm ops
#' }
#'
setGeneric("hydrar.normalize", function(hf, ...) {
  standardGeneric("hydrar.normalize")
})

setMethod("hydrar.normalize",
  signature(hf = "hydrar.frame"),
  function(hf, ...) {
    
    hfnames <- SparkR::colnames(hf)
    hftypes <- SparkR::coltypes(hf)
    
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
      stop(paste(unames, "columns not found in the input data"))
    }
    
    itypes <- hftypes[match(inames, hfnames)]
    
    
    # check that the data types of the imputed cols are of numeric
    # we have the constant string so take care of it
    binames <- inames[which(sapply(itypes, function(e) !e %in% c("numeric", "integer", "double")))]
    if (length(binames) >= 1) {
      stop(paste(binames, " input columns are not numeric and can't be imputed"))
    }
    
    # dynamically create command to be executed later
    rstr <- "SparkR::agg(hf"
    for (iname in inames) {
      
      mean_str <- paste("SparkR::", "mean" , "(as.sparkr.column(hf$", iname, "))", sep="")
      ndfname <- paste("mean_", iname, sep="")
      rstr <- paste(rstr, ", ", ndfname, " = ", mean_str, sep="")
      
      sd_str <- paste("SparkR::", "sd" , "(as.sparkr.column(hf$", iname, "))", sep="")
      ndfname <- paste("stddev_", iname, sep="")
      rstr <- paste(rstr, ", ", ndfname, " = ", sd_str, sep="")
      
    }
    
    rstr = paste(rstr, ")", sep="")
    
    
    # calc the mean of all the columns
    rhfstats <- eval(parse(text=rstr))
    hfstats <- SparkR::as.data.frame(rhfstats)
    
    mstr <- "SparkR::mutate(hf"
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
      mstr <- mstr %++% sprintf(", %s = (as.sparkr.column(hf$%s)-%s)/(2*%s)", new_col, iname, mean, sd)
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

#' @export
# NOTE: add the more specific arguement to ...
# NOTE: output must contain atleast same or more columns as input
setGeneric("hydrar.binning", function(hf, ...) {
  standardGeneric("hydrar.binning")
})

setMethod("hydrar.binning",
  signature(hf = "hydrar.frame"),
  function(hf, ...) {
    res_hf = hf; # change it in future
    meta_db <- list()
    warning("**WARNING** hydrar.binning is not implemented yet")
    res_hf
    list(data=res_hf, metadata=meta_db)
  }
)
