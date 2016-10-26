

# Choose dataset size

df_max_size = 90000
# df_max_size = 30

# Create Dataset
hf <- hydrar.data.gen.km(num_datapoints)
cache(hf)

# Preprocessing
survhf <- hydrar.ml.preprocess(
  hf,
  transformPath = "/tmp",
  binningAttrs = "X",
  numBins=10
)

# Transform data to hydrar matrix
survMatrix <- as.hydrar.matrix(survhf$data)
cache(survMatrix)

# Extablish formula for parsing
survFormula <- Surv(Timestamp, Censor) ~ Age

# Run kaplan meier on generated data
km <- hydrar.kaplan.meier(survFormula, data=survMatrix,
                         test=1, rho="none")
                         
# Produce Summary                          
summary = summary.hydrar.kaplan.meier(km)

# Compute Test Statistics
test = hydrar.kaplan.meier.test(km)

# exit R/HydraR
quit("no")