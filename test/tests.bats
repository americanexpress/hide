# /*
# * Copyright 2020 American Express Travel Related Services Company, Inc.
# *
# * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# * in compliance with the License. You may obtain a copy of the License at
# *
# * http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software distributed under the License
# * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# * or implied. See the License for the specific language governing permissions and limitations under
# * the License.
# *
# */

#!/usr/bin/env bats

@test "Check if the config file exists" {
    [[ -f ../../../config.cfg ]]
}

@test "Check if Program Base Path basePath is Setup in Config" {
    source ../../../config.cfg
    [ ! -z "$basePath" ]
}

@test "Check if hive queue name is Setup in Config" {
    source ../../../config.cfg
    [ ! -z "$QUEUE_NAME" ]
}

@test "Check if extractionDB is Setup in Config" {
    source ../../../config.cfg
    [ ! -z "$extractionDB" ]
}


@test "Check if extractionLoaction is Setup in Config" {
    source ../../../config.cfg
    [ ! -z "$extractionLoaction" ]
}

@test "Check if email is Setup in Config" {
    source ../../../config.cfg
    [ ! -z "$email" ]
}

@test "Check if Program Base Path basePath is absolute" {
    source ../../../config.cfg
    [[ "$basePath" = /* ]]
}

@test "Check if extractionLoaction path is absolute" {
    source ../../../config.cfg
    [[ "$extractionLoaction" = /* ]]
}