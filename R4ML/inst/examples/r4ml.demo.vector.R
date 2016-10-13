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

#load the hydrar 
library(HydraR)

# these are the different dataset we will analysis
# path <- "/user/data-scientist/airline/1987.csv"
# path <- "/user/data-scientist/airline/*.csv"


# we would like to limit the dataset to a size so that we can run test faster
df_max_size <- 1000
#df_max_size <- 100000

## use the following for the custom dataset
# path <- "/user/data-scientist/airline/1987.csv"
# df <- HydraR:::hydrar.read.csv(path, inferSchema=TRUE, header=TRUE)

# this example use the airline dataset shipped with HydraR
df <- as.hydrar.frame(airline)

# limit the number of rows so that we can control the size
df <- limit(df, df_max_size)
cache(df) # very important step otherwise the partition gets screw up

# convert to the hydrar frame
al_hf = as.hydrar.frame(df)

# Let air be the airline dataset
 
# Show/head
show(al_hf$ArrDelay)
head(al_hf$ArrDelay, 500)
x <- al_hf$ArrDelay
 
# Arithmetic operations
y <- sin(x)
z <- y ^ 2 + (cos(x) ^ 2)
head(z, 20)
as.integer(head(z, 20)) == 1
 
# Lit
round(z * lit('100')) == 100
 
# Correlation
corr(al_hf$ArrDelay, al_hf$DepDelay)
 
# IfElse
head(ifelse(al_hf$ArrDelay > 15, "Delayed", "Early"), 1000)
 
# Count distinct
countDistinct(al_hf$UniqueCarrier)
