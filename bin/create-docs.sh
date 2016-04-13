#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Script to create API docs for HydraR
# This requires `devtools` and `knitr` to be installed on the machine.

# After running this script the html docs can be found in 
# $HYDRAR_HOME/HydraR/html

set -o pipefail
set -e

# Figure out where the script is
export FWDIR="$(cd "`dirname "$0"`/../"; pwd)"
pushd $FWDIR

# Install the package (this will also generate the Rd files)
$FWDIR/bin/install-dev.sh

# Now create HTML files

# knit_rd puts html in current working directory
mkdir -p HydraR/html
pushd HydraR/html

Rscript -e ' libDir <- "../../lib"; library(HydraR, lib.loc=libDir); library(knitr); knit_rd("HydraR", links = tools::findHTMLlinks(paste(libDir, "HydraR", sep="/"))) '
popd

popd
