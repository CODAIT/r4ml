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

library("R4ML")
r4ml.session()

# Choose dataset size

df_size <- 90000
# df_size <- 30

# Generate survival data
r4ml_df <- r4ml.data.gen.km(df_size)

# Preprocessing
r4ml_df <- r4ml.ml.preprocess(
  data = r4ml_df,
  transformPath = "/tmp",
  binningAttrs = "X",
  numBins = 10)

# Transform data to r4ml matrix
survMatrix <- as.r4ml.matrix(r4ml_df$data)

# Establish formula for parsing
survFormula <- Surv(Timestamp, Censor) ~ Age + Race

# Run kaplan meier on generated data
km <- r4ml.kaplan.meier(data = survMatrix,
                        formula = survFormula,
                        conf.int = 0.95,
                        conf.type = "log",
                        error.type = "greenwood",
                        test.type = "wilcoxon")

# Produce Summary
summary <- summary(km)

# Compute Test Statistics
test <- r4ml.kaplan.meier.test(km)

# exit R/R4ML
r4ml.session.stop()
quit("no")
