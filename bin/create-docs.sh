#!/bin/bash

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

# Script to create API docs for R4ML
# This requires `devtools` and `knitr` to be installed on the machine.

# After running this script the html docs can be found in 
# $R4ML_HOME/R4ML/html

set -o pipefail
set -e

# Figure out where the script is
export FWDIR="$(cd "`dirname "$0"`/../"; pwd)"
pushd $FWDIR

# Install the package (this will also generate the Rd files)
$FWDIR/bin/install-dev.sh

# Now create HTML files

# knit_rd puts html in current working directory
mkdir -p R4ML/html
pushd R4ML/html

echo "Creating man pages"
Rscript -e ' libDir <- "../../lib"; library(R4ML, lib.loc=libDir); library(knitr); knit_rd("R4ML", links = tools::findHTMLlinks(paste(libDir, "R4ML", sep="/"))) '
popd

# create vignettes (currently none to make)
# pushd R4ML/vignettes
# echo "Creating Vignettes"
# Rscript -e ' libDir <- "../../lib"; library(R4ML, lib.loc=libDir); library(devtools); devtools::build_vignettes()'
# popd

popd