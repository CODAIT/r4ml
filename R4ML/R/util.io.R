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
#
# This file contains io utility functions that can be used all
# accross the project
#
#

## r4ml.msg returns the formated string
r4ml.gen.logger <- function(ilevel) {
  force(ilevel)
  ilevel <- ifelse(missing(ilevel), "", ilevel)
  def_logsrc <- r4ml.env$PACKAGE_NAME
  force(def_logsrc)
  
  function(src, ...) {
    
    # cleanin the input
    msg <- unlist(list(...))
    src <- ifelse(missing(src), def_logsrc, src)
    if (missing(msg)) {
      msg <- ""
    }
    
    if (exists("r4ml.logger", envir=.GlobalEnv)) {
      if (r4ml.logger$isLoggable(ilevel)) {
        # if r4ml logger has been initialized user it
        code.logging <- sprintf("r4ml.logger$%s(msg, src)", tolower(ilevel))
        eval(parse(text=code.logging))
      }
    } else {
      # we are in the initialization of r4ml, so this covers the special case
      
      # unify the messages
      nmsg <- ""
      if (length(msg) == 1) {
        nmsg <- sprintf("%s[%s]: %s", ilevel, src, msg)
      } else {
        nmsg <- sprintf("%s[%s]: [vector]: %s ", ilevel, src, paste(msg, collapse=", "))
      }
      
      # find the function to be called for delegating
      f_name <- switch(ilevel,
                  "TRACE" = "message",
                  "DEBUG" = "message",
                  "INFO" = "message",
                  "WARN" = "warning",
                  "ERROR" = "stop",
                  "FATAL" = "stop"
                )
      call_pat <- ""
      if (f_name == "warning" || f_name == "stop") {
        call_pat <- ", call. = FALSE"
      }
      code.logging <- sprintf("%s(nmsg %s)", f_name, call_pat)
      
      # run the delegated code
      eval(parse(text=code.logging))
    }
  }
}

# various logger generated from gen
r4ml.trace <- r4ml.gen.logger("TRACE")
r4ml.debug <- r4ml.gen.logger("DEBUG")
r4ml.info <- r4ml.gen.logger("INFO")
r4ml.warn <- r4ml.gen.logger("WARN")
r4ml.err <- r4ml.gen.logger("ERROR")
r4ml.fatal <- r4ml.gen.logger("FATAL")

#' r4ml.setLogLevel
#' @description Set log level for the current session.
#' @param level the level to be used. The various log levels that are supported are "ALL", "TRACE",
#' "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF" in the increasing priority.
#' @param set.java.log logical variable to set java log level. Default is FALSE.
#' @details When R4ML session is started, the defalult log level is set to "INFO". This function allows to
#' set the desired log level after the session is started. The java log level used by SystemML is set to ERROR
#' by default. That behaviour can also be changed by setting the set.java.log flag to TRUE.
#' @usage r4ml.setLogLevel("WARN")
#' @examples \dontrun{
#' r4ml.session()
#' r4ml.setLogLevel("WARN")
#' }
# allow user to set the log level
r4ml.setLogLevel <- function(level, set.java.log = FALSE) {
  if(!exists("r4ml.logger", envir=.GlobalEnv)) {
    stop("R4ML session does not exist.")
  }
  r4ml.logger$setLevel(level, ...)
}

#' r4ml.getLogLevel
#' @description Get log level for the current session.
#' @usage r4ml.getLogLevel()
# allow use to get current log level
r4ml.getLogLevel <- function(level) {
  if (!exists("r4ml.logger", envir=.GlobalEnv)) {
    stop("R4ML session does not exist.")
  }
  r4ml.logger$getLevel()
}

# in case we want to show the whole object. This might go away in future
r4ml.infoShow <- function(source, message) {
  if (exists("r4ml.logger", envir=.GlobalEnv)) {
    if (!r4ml.logger$isLoggable("INFO")) {
      return
    }
  } 
  source <- ifelse(missing(source), "", source)
  message <- ifelse(missing(message), "", message)
  
  cat(sprintf("INFO[%s]", source))
  show(message)
  cat("\n")
}

# in case we want to show the whole object. This might go away in future
r4ml.debugShow <- function(source, message) {
  if (exists("r4ml.logger", envir=.GlobalEnv)) {
    if (!r4ml.logger$isLoggable("DEBUG")) {
      return
    }
  }
  source <- ifelse(missing(source), "", source)
  message <- ifelse(missing(message), "", message)

  cat(sprintf("DEBUG[%s]", source))
  show(message)
  cat("\n")
}

# create the r4ml.fs object
create.r4ml.fs <- function() {
  logSource <- "r4ml.fs"
  
  fs.obj <- NA
  if (r4ml.fs.mode() == "local") {
    fs.obj <- LinuxFS$new()
  } else if (r4ml.fs.mode() == "cluster") {
    fs.obj <- HadoopFS$new()
  } else {
    r4ml.fatal(logSource, "Unknown file system")
  }
  
  return(fs.obj)
}

# find the fs mode of r4ml
r4ml.fs.mode <- function() {
  logSource <- "r4ml.fs.mode"

  # yarn client and yarn both are the cluster mode
  if (SparkR::sparkR.conf()$spark.master == "yarn-client") {
    return("cluster")
  }
  if (SparkR::sparkR.conf()$spark.master == "yarn") {
    return("cluster")
  }
  
  # also if user specify the master url, then it is the cluster mode too
  if (substr(SparkR::sparkR.conf()$spark.master, 1, 8) == "spark://") {
    return("cluster")
  }
  
  #user input or env has local[*] kind of settings
  if (substr(SparkR::sparkR.conf()$spark.master, 1, 5) == "local") {
    return("local")
  }
  
  # if the input is not one of the above it is most liklely some other kind of spark cluster
  # TODO discuss, if it has to be local?
  r4ml.warn(logSource, "Unable to determine file system. Defaulting to cluster mode.")
  return("cluster")
}

# predicate to check if it is the local mode
# TODO discuss if we want it to be based on class hierarchy of FileSystem class
is.r4ml.fs.local <- function() {
  return (ifelse(r4ml.fs.mode()=="local", TRUE, FALSE))
}

# predicate to check if it is the cluster mode
# TODO discuss if we want it to be based on class hierarchy of FileSystem class
is.r4ml.fs.cluster <- function() {
  return (ifelse(r4ml.fs.mode()=="cluster", TRUE, FALSE))
}

# check if we have the valid file system
is.r4ml.fs.valid <- function() {
  fs_mode <- r4ml.fs.mode()
  if (fs_mode == "cluster") {
    return(TRUE)
  } else if (fs_mode == "local") {
    return(TRUE)
  } else {
    return(FALSE)
  }  
}

#' r4ml.read.csv
#' @description Returns a R data.frame in local mode or a SparkR DataFrame in cluster mode.
#' @param file path to the input file
#' @param header logical
#' @param stringsAsFactors logical
#' @param inferSchema logical
#' @param na.strings a vector of strings which should be interpreted as NA
#' @param schema (cluster mode) the SparkR scheme
#' @param sep field separator character, supported in local mode
#' @param ... (optional)
#' @export
r4ml.read.csv <- function(
  file, header = FALSE, stringsAsFactors = FALSE, inferSchema = FALSE, sep = ",",
   na.strings = "NA", schema = NULL, ...
){
  logSource <- "r4ml.read.csv"
  
  # extra check to make sure that we have one of the valid mode
  if (!is.r4ml.fs.valid()) {
    r4ml.err(logSource, "Invalid r4ml filesystem found")  
  }
  
  # in the local mode, we just delegate to the R's read.csv
  if(is.r4ml.fs.local()) {
    if (!is.null(schema)) {
      r4ml.err(logSource, "schema not supported in local mode")
    }
    df <- utils::read.csv(file, header = header, stringsAsFactors = stringsAsFactors,
                          sep = sep, na.strings = na.strings, ...)
    return(df)
  }
  
  # we need to pass in these arguments as strings
  if (!typeof(header) == "logical") {
    r4ml.err(logSource, "!header : invalid argument type")
  }
  if (!typeof(stringsAsFactors) == "logical") {
    r4ml.err(logSource, "!stringsAsFactors : invalid argument type")
  }
  if (!typeof(inferSchema) == "logical") {
    r4ml.err(logSource, "!inferSchema : invalid argument type")
  }

  header_val <- ifelse(header, "true", "false")
  stringsAsFactors_val <- ifelse(stringsAsFactors, "true", "false")
  inferSchema_val <- ifelse(inferSchema, "true", "false")
  
  # old way of reading using the external pacakge com.databricks.spark.csv
  # df <- SparkR::read.df(sysmlSqlContext,
  #                       file,
  #                       source = "com.databricks.spark.csv",
  #                       header = header_val,
  #                       stringsAsFactors = stringsAsFactors_val,
  #                       inferSchema = inferSchema_val)
  src = "csv" # we always default to csv user can use read.df for other things
  df <- SparkR::read.df(
          file,
          source = src,
          header = header_val,
          inferSchema = inferSchema_val,
          na.strings = na.strings,
          delimiter = sep,
          ...
        )
  return(df)
}

#' R4ML Logging class
#' 
#' @docType class
#' @importFrom R6 R6Class
#' @format A Unified logging utility which control R4ML, SparkR, SystemML 
#' log levels.The various log levels that are supported are "ALL", "TRACE",
#' "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF" in the increasing priority.
#' Like other log services, setting a log level to say WARN enables all the
#' higher level logging (i.e ERROR FATAL) but disables lower level logging
#' (i.e TRACE, DEBUG, INFO).
#' NOTE: ALL and OFF are used to enable all the logs or disable all the logs
#' respectively. Since SystemML and Spark related JVM logging level is for 
#' higer level debug, user have different ways to control jvm and R logging 
#' seperately.
#' @section Methods:
#' \describe{
#'   \item{\code{isValidLevel(logLevel)}}
#'      {check if the log level is valid. Valid log levels are"ALL", "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF"}
#'   \item{\code{setLevel(logLevel, force_java = TRUE)}}
#'      {set the R4ML logger to the specified level. force_java=FALSE will not update the java log level}
#'  \item{\code{getLevel(is_java = FALSE)}}
#'      {get the R4ML loglevel. If is_java is TRUE then return the jvm log level. Usually both of them will be in sync but there is no gurantee}
#'  \item{\code{isLoggable(logLevel)}}
#'      {See if this log level will log the message or not}
#'  \item{\code{trace(msg, root = "")}}
#'      {trace level logging for message with the a given root name}
#'  \item{\code{debug(msg, root = "")}}
#'      {debug level logging for message with the a given root name}
#'  \item{\code{info(msg, root = "")}}
#'      {info level logging for message with the a given root name}
#'  \item{\code{warn(msg, root = "")}}
#'      {warn level logging for message with the a given root name}
#'  \item{\code{error(msg, root = "")}}
#'      {error level logging for message with the a given root name}
#'  \item{\code{fatal(msg, root = "")}}
#'      {fatal level logging for message with the a given root name}
#'  \item{\code{message(msg, root = "", ilevel = "")}}
#'      {this is helper routine for other logger. It can be use for debug also
#'       It returns the string that will be printed by other logger}
#'}      
#' 
#' @examples \dontrun{
#'   mylogger <- R4ML:::Logging$new();
#'   # default levels
#'   level <- mylogger$getLevel();
#'   # change the log level
#'   mylogger$setLevel("ERROR")
#'   mylogger$setLevel(level)
#' }
#' 
Logging <- R6::R6Class(
  "Logging",
  public = list(
    # name of the instance
    name = "Logging",
    
    # default current level
    level = "INFO",
    
    # all the valid levels. note: this is same as the java so that we can control from the same api
    levels = c("ALL", "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF"),
    
    # ptr to the java logger
    jlogger = NA,
    
    # ctor
    initialize = function(name = "R4ML", level = "INFO") {
      self$name <- name
      self$level <- level
      self$jlogger <- get("jlogger", .GlobalEnv)
    },
    
    # is current string representation represent correct level
    isValidLevel = function(val) {
      return (val %in% self$levels)
    },
    
    # set the current log level. note: r4ml and java may have different levels
    setLevel = function(val, force_java=FALSE) {
      if (!self$isValidLevel(val)) {
        levels_str <- paste(self$levels, collapse = ",", sep = ",")
        msg <- sprintf("invalid level '%s'.Only %s levels are supported", val, levels_str)
        warning(msg)
        return
      }
      if (force_java) { 
         self$jlogger$setLevel(val)
      }
      self$level <- val
    },
    
    # get the current level. note: r4ml and java may have different levels
    getLevel = function(is_java=FALSE) {
      if(!is_java) {
        self$level
      } else {
        self$jlogger$getLevel()
      }
    },
    
    # logically compare two levels
    compare = function(level1, level2) {
      if (!self$isValidLevel(level1) || !(self$isValidLevel(level2))) {
         return(NA)
      }
      i1 <- match(level1, self$levels)
      i2 <- match(level2, self$levels)
      return (i1 - i2);
    },
    
    # check if current level is loggable
    isLoggable = function(level) {
      cmp <- self$compare(self$level, level)
      if (!is.na(cmp) && cmp <= 0) {
        return(TRUE)
      } else {
        return(FALSE)
      }
    },
    
    # returns the string representation of the user msg for log's root and log's level
    message = function(msg, root="", ilevel="") {
      if (!self$isLoggable(ilevel)) { return}
      nmsg <- ""
      if (length(msg) == 1) {
        nmsg <- sprintf("%s[%s]: %s", ilevel, root, msg)
      } else {
        nmsg <- sprintf("%s[%s]: [vector]: %s", ilevel, root, paste(msg, collapse=", "))
      }
      nmsg 
    },
    
    # trace level logging
    trace = function(msg, root="") {
      message(self$message(msg, root, "TRACE"))
    },
    
    # debug level logging
    debug = function(msg, root="") {
      message(self$message(msg, root, "DEBUG"))
    },
    
    # info level logging
    info = function(msg, root="") {
      message(self$message(msg, root, "INFO"))
    },
    
    # warn level logging
    warn = function(msg, root="") {
      warning(self$message(msg, root, "WARN"), call. = FALSE)
    },
    
    # error level logging
    error = function(msg, root="") {
      stop(self$message(msg, root, "ERROR"), call. = FALSE)
    },
    
    # fatal level logging
    fatal = function(msg, root="") {
      stop(self$message(msg, root, "FATAL"), call. = FALSE)
    }
    
))

# There is the need for support for various file system i.e Hadoop and Linux.
# This class creates the abstraction around the fileSystem. This will be the core
# and will be used in many applications

#' R4ML FileSystem Class
#' 
#' @docType class
#' @importFrom R6 R6Class
#' @format There is the need for support for various file system i.e Hadoop and Linux.
#' This class creates the abstraction around the fileSystem. This will be the 
#' core and will be used in many applications. This is an abstract class, 
#' so users are advised to use the concrete implementation i.e
#' R4ML:::LinuxFS and R4ML::HadoopFS (see examples below)
#' @section Methods:
#' \describe{
#'   \item{\code{create(file_path, is_dir=TRUE)}}
#'      {Create the zero length file or directory. 
#'       Returns TRUE on sucess and FALSE on failure}
#'   \item{\code{remove(file_path)}}
#'      {remove the file_path (file/dir) from the FileSystem}
#'  \item{\code{exists(file_path)}}
#'      {check if the file_path (file/dir) exists on the FileSystem}
#'  \item{\code{sh.exec()}}
#'      {system execution of a unix like shell cmd, Note that in all the filesystem,
#'       this is always call the underlying os}
#'  \item{\code{user.home()}}
#'      {Every filesystem has it's own home dir defined, so this creates the uniform interface}
#'  \item{\code{uu_name()}}
#'      {universal unique file full path: note files are not created. see also tempdir()}  
#'  \item{\code{tempdir()}}
#'      {create the universal unique file full path: note this uses internall uu_name()}    
#' }      
#' 
#' @examples \dontrun{
#'   my.r4ml.fs <- NA
#'   if (r4ml.fs.local()) {
#'     my.r4ml.fs <- R4ML:::LinuxFS$new()
#'   } else if (R4ML:::r4ml.fs.cluster) {
#'     my.r4ml.fs <- R4ML:::HadoopFS$new()
#'   } else {
#'     stop("Unknown file system")
#'   }
#'   
#'   # create the unique file name
#'   uu_name <- my.r4ml.fs$uu_name();
#'   cat("testing universal unique file name: ", uu_name)
#'
#'   # create the uniq file, it should return TRUE
#'   my.r4ml.fs$create(uu_name)
#'
#'   # check if the file exists, it should return TRUE
#'   my.r4ml.fs$exists(uu_name)
#'
#'   # remove the file, it should return TRUE
#'   my.r4ml.fs$remove(uu_name)
#'
#'   # check again, this time the file shouldn't exists and should return FALSE
#'   my.r4ml.fs$exists(uu_name)
#'
#'   # the file/dir must be deleted later automatically on sys.exit
#'   temp_dir <- my.r4ml.fs$tempdir()
#'   
#'   # returns the home dir of the user
#'   user_home <- my.r4ml.fs$user.home()
#' }
#'
FileSystem <- R6::R6Class(
  "FileSystem",
  public = list(
    
    name = "FileSystem",
    
    # ctor
    initialize = function() {
       private$TEMPS <- new.env(parent=emptyenv())
       private$reg.removeTempsOnExit(private$TEMPS)
    },
    
    # remove the file 
    remove = function(f) {
      r4ml.fatal("", "FileSystem$remove not implemented in abstract class")
    },
    
    # check for existence
    exists = function(f) {
      r4ml.fatal("", "FileSystem$exists not implemented in abstract class")
    },
    
    # create the file
    create = function(f, is_dir = TRUE) {
      r4ml.fatal("", "FileSystem$create not implemented in abstract class")
    },
    
    # user home dir
    user.home = function() {
      r4ml.fatal("", "FileSystem$user.home not implemented in abstract class")
    },
    
    # universal unique file name: note files are not created
    uu_name = function(prefix = "/tmp", suffix = "") {
      # create uniq directory with the prefix
      fuuid <- uuid::UUIDgenerate()
      tdir <- file.path(prefix, fuuid, suffix)
      tdir
    },
    
   # temp dir which which will be automatically deleted on sys exit  
   tempdir = function(prefix = "/tmp", suffix = "") {
      # create uniq directory with the prefix
      tdir <- self$uu_name(prefix, suffix)
      
      # create the directory on the filesystem
      self$create(tdir, is_dir = TRUE)
      
      # register it to be deleted automatically on exit
      private$removeOnSysExit(tdir)
      tdir
    },
    
    # system execution of a cmd
    sh.exec = function(cmd) {
      r4ml.debug("Executing the shell cmd: " %++% cmd)
      
      # execute the system call
      status <- system(cmd)
      
      if (status != 0) {
        r4ml.warn("Failed to execute shell cmd: " %++% cmd)
      }
      
      ifelse(status == 0, TRUE, FALSE)
    }
  ),
  
  private = list(
    TEMPS = "",
    
    # register the file to be cleaned later
    removeOnSysExit = function(f) {
      # update the list of tempfiles to be removed on exit
      assign(f, class(self), envir=private$TEMPS)
    },
    
    # clean registrater 
    reg.removeTempsOnExit = function(f2rm_env) {
      # register the temp directory to be cleaned at the time of 
      # system exit
      reg.finalizer(
        f2rm_env, 
        function(temps) {
          fs_name <- class(self)[1]
          pkg <- r4ml.env$PACKAGE_NAME
          logsrc <- sprintf("%s.%s", pkg, fs_name)
          r4ml.trace(logsrc,"Deleting the temporary fileSystem Resources")
          files <- ls(temps)
          for (f in files) {
            if (self$exists(f)) {
              r4ml.info(logsrc, sprintf("Deleting the [%s] temporary resource '%s'",fs_name, f))
              self$remove(f)
            } else {
              r4ml.warn(logsrc, sprintf("[%s] temporary resource '%s' doesn't exists : ", fs_name, f))
            } 
          }
        },
        onexit = TRUE)
    }
  )
)

# Linux based FileSystem. It is a derived class implementing factory method. 
# see R4ML:::FileSystem class too
LinuxFS <- R6::R6Class(
  "LinuxFS",
  inherit = FileSystem,
  public = list(
    
    name = "LinuxFS",
    
    # override base. remove the linux file/dir 
    remove = function(f) {
      r4ml.debug(self$name, "removing file/dir " %++% f)
      status <- base::unlink(f, recursive = TRUE)
      ifelse(status == 0, TRUE, FALSE)
    },
    
    # override base. check for existence of linux file/dir
    exists = function(f) {
      r4ml.debug(self$name, "checking for file existence:" %++% f)
      base::file.exists(f)
    },
    
    # override base. create the linux file/dir
    create = function(f, is_dir = TRUE) {
      if (is_dir) {
        r4ml.debug(self$name, "creating dir" %++% f)
        base::dir.create(f, recursive = TRUE)
      } else {
        r4ml.debug(self$name, "creating file" %++% f)
        base::file.create(f)
      }
    },
    
    # user home dir
    user.home = function() {
      if (is.null(Sys.getenv("HOME")) || Sys.getenv("HOME") == "") {
        stop("environmental variable HOME not defined")
      }
      return (Sys.getenv("HOME"))
    }
  )
)

# Hadoop based file system. It is a derived class implementing factory method. 
# see R4ML:::FileSystem class too
HadoopFS <- R6::R6Class(
  "HadoopFS",
  inherit = FileSystem,
  public = list(
    
    name = "HadoopFS",
    
    # override base. remove the hadoop file/dir
    remove = function(f) {
      r4ml.debug(self$name, " removing file:" %++% f)
      hcmd <- sprintf("hdfs dfs -rm -r %s", f)
      self$sh.exec(hcmd)
    },
    
    # override base. check for existence of hadoop file/dir
    exists = function(f) {
      r4ml.debug(self$name, "checking for file existence:" %++% f)
      hcmd <- sprintf("(hdfs dfs -test -d %s) || (hdfs dfs -test -d %s)", f, f)
      self$sh.exec(hcmd)
    },
    
    # override base. create hadoop file/dir
    create = function(f, is_dir = TRUE) {
      if (is_dir) {
        r4ml.debug(self$name, "creating dir" %++% f)
        hcmd <- sprintf("hdfs dfs -mkdir -p %s", f)
        self$sh.exec(hcmd)
      } else {
        r4ml.debug(self$name, "creating file" %++% f)
        hcmd <- sprintf("hdfs dfs -touchz %s", f)
        self$sh.exec(hcmd)
      }
    },
    
    # user home dir
    user.home = function() {
      if (is.null(Sys.getenv("USER")) || Sys.getenv("USER") == "") {
        stop("environmental variable USER not defined")
      }
      return (file.path("", "user", Sys.getenv("USER")))
    }
  )
)

