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
#' @include zzz.R
NULL


#'
#' A Reference Class that represent log4j Logger
#'
#' Logger class help one control the logging levels in the R console.
#' We have the global logger called logger which should be use to set the loglevel
#' at the underlying java/scala level
#'
#' @family Java Utils
#'
#' @field env An R environment that stores bookkeeping states of the class
#'        along with java ref corresponding to jvm
#' @examples
#' \dontrun{
#'    logger = log4j.Logger$new()
#'    logger.setLevel("WARN")
#' }
#'

log4j.Logger <- setRefClass("log4j.Logger",
  fields = list(env="environment"),
  methods = list(
    initialize = function(name = "org") {
      env <<- new.env()
      createLogger <- function(level) {
        level_jref <- SparkR:::callJStatic("org.apache.log4j.Level", "toLevel", level)
        level_jref
      }
      env$jref <<-SparkR:::callJStatic("org.apache.log4j.Logger", "getLogger", name)
      levels <- c("INFO", "ERROR", "WARN", "DEBUG", "ALL", "OFF")
      level_jobjs <- sapply(levels, createLogger)
      #names(level_jobjs) <- levels
      sapply(levels, function(level) {assign(level, level_jobjs[[level]], env)})
      #env$INFO <<- SparkR:::callJStatic("org.apache.log4j.Level", "toLevel", "INFO")
      #env$ERROR <<- SparkR:::callJStatic("org.apache.log4j.Level", "toLevel", "ERROR")
      #env$WARN <<- SparkR:::callJStatic("org.apache.log4j.Level", "toLevel", "WARN")
      #env$DEBUG <<- SparkR:::callJStatic("org.apache.log4j.Level", "toLevel", "DEBUG")
      #env$ALL <<- SparkR:::callJStatic("org.apache.log4j.Level", "toLevel", "ALL")
      #env$OFF <<- SparkR:::callJStatic("org.apache.log4j.Level", "toLevel", "OFF")
    },

    finalize = function() { # cleanup is not necessary
    },

    setLevel = function(level) {
      '\\tabular{ll}{
         Description:\\tab \\cr
           \\tab set the log levels. Supported options are ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, TRACE_INT, WARN \\cr
       }'
      level_jref = get(level, .self$env)
      SparkR:::callJMethod(env$jref, "setLevel", level_jref)
      logger$getLevel()
    },

    getLevel = function() {
      '\\tabular{ll}{
         Description:\\tab \\cr
           \\tab gets the log level\\cr
       }'
      level_jref <- SparkR:::callJMethod(env$jref, "getLevel")
      return(SparkR:::callJMethod(level_jref, "toString"))
    }
  )
)


#'
#' A Reference Class that represent java HashMap which lives in the spark jvm memory
#'
#' HashMap is used for passing in arguements between java and R code
#'
#' @family Java Utils
#' @export
#' @field env An R environment that stores bookkeeping states of the class
#'        along with java ref corresponding to jvm
#' @examples \dontrun{
#'    ja = java.ArrayList$new()
#'    ja.add("a")
#'}
#'
#'
java.ArrayList <- setRefClass("java.ArrayList",
  fields = list(env="environment"),
  methods = list(
    initialize = function() {
      env <<- new.env()
      env$jref <<-SparkR:::newJObject("java.util.ArrayList")
    },

    finalize = function() { # cleanup is not necessary
      SparkR:::cleanup.jobj(env$jref)
    },

    add = function(value) {
      '\\tabular{ll}{
      Description:\\tab \\cr
      \\tab Add the value in the ArrayList \\cr
      }'
      SparkR:::callJMethod(env$jref, "add", value)
    },

    get = function(index) {
      '\\tabular{ll}{
      Description:\\tab \\cr
      \\tab get the value at the index from ArrayList \\cr
      }'
      SparkR:::callJMethod(env$jref, "get", as.integer(index))
    },

    size = function() {
      '\\tabular{ll}{
      Description:\\tab \\cr
      \\tab get the size of the array list \\cr
      }'
      SparkR:::callJMethod(env$jref, "size")
    }
  )
)
