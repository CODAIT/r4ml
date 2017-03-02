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

library(HydraR)
hydrar.session()

# Choose dataset size

df_max_size = 90000
# df_max_size = 30

# Create Dataset
hf <- hydrar.data.gen.km(df_max_size)
ignore <- cache(hf)

# Preprocessing
survhf <- hydrar.ml.preprocess(
  hf,
  transformPath = "/tmp",
  binningAttrs = "X",
  numBins=10
)

# Transform data to hydrar matrix
survMatrix <- as.hydrar.matrix(survhf$data)
ignore <- cache(survMatrix)

# Establish formula for parsing
survFormula <- Surv(Timestamp, Censor) ~ Age

# Run kaplan meier on generated data
km <- hydrar.kaplan.meier(survFormula, data=survMatrix,
                         test=1, rho="none")
                         
# Produce Summary                          
summary <- summary(km)

# Compute Test Statistics
test = hydrar.kaplan.meier.test(km)

# exit R/HydraR
hydrar.session.stop()
quit("no")
