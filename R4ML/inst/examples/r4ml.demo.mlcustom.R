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

# load the library
library(R4ML)
r4ml.session()

# lets create function to do the one hot encoding using the systemML dml .
# the basic steps are 
# R4ML allows one to create the custom ML utils and algorithms in the systemML
# dml language. In future, we might have the native support for the R language 
# via the DSL. To illustrate the concept, lets try to implement one ML utils via
# the custom approach. We know that Spark has the OneHotEncoding Transformer 
# and for the efficiency reason, we are suppose to create the wrapper over it 
# rather than implementing using the api from sparkR.  # However we can very 
# easily implement it using the [SystemML dml] 
# (http: //apache.github.io/incubator-systemml/dml-language-reference.html)
# with one liner i.e 
##  dml <-  'Y = table(seq(1, N, 1), X)' 
##  where X is input vector and N is the number of rows in X. 
##        Y is one hot encoded output of X
# The detail code is in this example 

# lets first define the systemML dml code for doing one hot encoding
## BEGIN one hot encoding function
udf_onehot <- function(hm) {
  dml= '
    # encode dml function for one hot encoding
    encode_onehot = function(matrix[double] X) return(matrix[double] Y) {
N = nrow(X)
      Y = table(seq(1, N, 1), X)
    }
    # a dummy read, which allows sysML to attach variables
    dummy_fileX = ""
    X = read($dummy_fileX)
    # onehot encode code
    Y = encode_onehot(X)
    # a dummy write, which allows sysML to attach varibles
    dummy_fileY = ""
    write(Y, $dummy_fileY)
  '
 
  # call the sysml connector to execute the code in the cluster after # matrix optimization
  outputs <- sysml.execute(
    dml = dml, # dml code
    X = hm, # attach the input r4ml matrix to X var in dml 
    "Y" # attach the output Y from dml
  )
  Y = outputs[['Y']] # get the output dataframes
  # assign the proper column names
  Y_names <- paste(names(hm), 1:length(names(Y)), sep=":") 
  SparkR::colnames(Y) <- Y_names
  Y
}
## END one hot encoding function

# BEGIN simple data set
# a temporary data frame which is already being recoded
tmp_df <- data.frame(recode=c(5,4,2))
# since most of the sysml interface is via the matrix.
 
tmp_hm <- as.r4ml.matrix(tmp_df) # call the previously create onehot udf
tmp_onehot_hm <- udf_onehot(tmp_hm)
# lets see it's content
showDF(tmp_onehot_hm)
# END simple data set


## BEGIN airline dataset
# now lets run it on the airline dataset

# these are the different dataset we will analysis
# path <- "/user/data-scientist/airline/1987.csv"
# path <- "/user/data-scientist/airline/*.csv"


# we would like to limit the dataset to a size so that we can run test faster
df_max_size <- 1000
#df_max_size <- 100000

# set the predictors and response variables
attr2process <- c("Month")

## use the following for the custom dataset
# path <- "/user/data-scientist/airline/1987.csv"
# df <- R4ML:::r4ml.read.csv(path, inferSchema=TRUE, header=TRUE)

# this example use the airline dataset shipped with R4ML
al_df <- as.r4ml.frame(airline)

# limit the number of rows so that we can control the size
al_df <- limit(al_df, df_max_size)
ignore <- cache(al_df) # very important step otherwise the partition gets screw up

# convert to the r4ml frame
al_df <- SparkR::select(al_df, attr2process)
ignore <- cache(al_df)
al_hm <- as.r4ml.matrix(al_df)

al_onehot_hm <- udf_onehot(al_hm)
# lets see it's content
showDF(al_onehot_hm)


## END airline dataset

r4ml.session.stop()
q("no")
