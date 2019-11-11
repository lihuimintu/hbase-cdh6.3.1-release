#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# first updated from upstream dev-support/hbase-personality on commit
#     41be3bc2cc42565335d55f553c536b1fce2aa023
#
#   - Removed hadoop-check test, since CDH only has 1 Hadoop version
#   - updated branch-default to current merge branch of C6 development
#   - removed unused GitHub pointer
#   - updated JIRA regex to allow for CDH jiras.
#   - updated flaky handling to presume ENV variable with tests instead of URL.
#   - removed commented out zombie detector
#
#
# You'll need a local installation of
# [Apache Yetus' precommit checker](http://yetus.apache.org/documentation/0.1.0/#yetus-precommit)
# to use this personality.
#
# Download from: http://yetus.apache.org/downloads/ . You can either grab the source artifact and
# build from it, or use the convenience binaries provided on that download page.
#
# To run against, e.g. HBASE-15074 you'd then do
# ```bash
# test-patch --personality=dev-support/hbase-personality.sh HBASE-15074
# ```
#
#
# pass the `--jenkins` flag if you want to allow test-patch to destructively alter local working
# directory / branch in order to have things match what the issue patch requests.

personality_plugins "all"

if ! declare -f "yetus_info" >/dev/null; then

  function yetus_info
  {
    echo "[$(date) INFO]: $*" 1>&2
  }

fi

## @description  Globals specific to this personality
## @audience     private
## @stability    evolving
function personality_globals
{
  BUILDTOOL=maven
  #shellcheck disable=SC2034
  PROJECT_NAME=hbase
  #shellcheck disable=SC2034
  PATCH_BRANCH_DEFAULT=${GERRIT_BRANCH:-"cdh6-branch-2-merged"}
  #shellcheck disable=SC2034
  JIRA_ISSUE_RE='^(HBASE|CDH)-[0-9]+$'

  # TODO use PATCH_BRANCH to select jdk versions to use.

  # Override the maven options
  MAVEN_OPTS="${MAVEN_OPTS:-"-Xmx3100M"}"

}

## @description  Queue up modules for this personality
## @audience     private
## @stability    evolving
## @param        repostatus
## @param        testtype
function personality_modules
{
  local repostatus=$1
  local testtype=$2
  local extra=""

  yetus_debug "Personality: ${repostatus} ${testtype}"

  clear_personality_queue

  extra="-DHBasePatchProcess"

  # BUILDMODE value is 'full' when there is no patch to be tested, and we are running checks on
  # full source code instead. In this case, do full compiles, tests, etc instead of per
  # module.
  # Used in nightly runs.
  # If BUILDMODE is 'patch', for unit and compile testtypes, there is no need to run individual
  # modules if root is included. HBASE-18505
  if [[ "${BUILDMODE}" == "full" ]] || \
     [[ ( "${testtype}" == unit || "${testtype}" == compile ) && "${MODULES[*]}" =~ \. ]]; then
    MODULES=(.)
  fi

  if [[ ${testtype} == mvninstall ]]; then
    # shellcheck disable=SC2086
    personality_enqueue_module . ${extra}
    return
  fi

  if [[ ${testtype} == findbugs ]]; then
    # Run findbugs on each module individually to diff pre-patch and post-patch results and
    # report new warnings for changed modules only.
    # For some reason, findbugs on root is not working, but running on individual modules is
    # working. For time being, let it run on original list of CHANGED_MODULES. HBASE-19491
    for module in "${CHANGED_MODULES[@]}"; do
      # skip findbugs on hbase-shell and hbase-it. hbase-it has nothing
      # in src/main/java where findbugs goes to look
      if [[ ${module} == hbase-shell ]]; then
        continue
      elif [[ ${module} == hbase-it ]]; then
        continue
      else
        # shellcheck disable=SC2086
        personality_enqueue_module ${module} ${extra}
      fi
    done
    return
  fi

  # If EXCLUDE_TESTS/INCLUDE_TESTS is set,
  # use -Dtest.exclude.pattern/-Dtest to exclude/include the
  # tests respectively.
  if [[ ${testtype} = unit ]]; then
    # if the modules include root, skip all the submodules HBASE-18505
    if [[ "${CHANGED_MODULES[*]}" =~ \. ]]; then
      CHANGED_MODULES=(.)
    fi

    extra="${extra} -PrunAllTests"
    if [[ -n "${EXCLUDED_TESTS}" ]]; then
      extra="${extra} -Dtest.exclude.pattern=${EXCLUDED_TESTS}"
    elif [[ -n "$INCLUDED_TESTS" ]]; then
      extra="${extra} -Dtest=${INCLUDED_TESTS}"
    fi

    # Inject the jenkins build-id for our surefire invocations
    # Used by zombie detection stuff, even though we're not including that yet.
    if [ -n "${BUILD_ID}" ]; then
      extra="${extra} -Dbuild.id=${BUILD_ID}"
    fi
  fi

  for module in "${CHANGED_MODULES[@]}"; do
    # shellcheck disable=SC2086
    personality_enqueue_module ${module} ${extra}
  done
}

###################################################
# Below here are our one-off tests specific to hbase.
# TODO break them into individual files so it's easier to maintain them?

# TODO line length check? could ignore all java files since checkstyle gets them.

###################################################

# TODO if we need the protoc check, we probably need to check building all the modules that rely on hbase-protocol
add_test_type hbaseprotoc

## @description  hbaseprotoc file filter
## @audience     private
## @stability    evolving
## @param        filename
function hbaseprotoc_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.proto$ ]]; then
    add_test hbaseprotoc
  fi
}

## @description  hadoopcheck test
## @audience     private
## @stability    evolving
## @param        repostatus
function hbaseprotoc_rebuild
{
  declare repostatus=$1
  declare i=0
  declare fn
  declare module
  declare logfile
  declare count
  declare result

  if [[ "${repostatus}" = branch ]]; then
    return 0
  fi

  if ! verify_needed_test hbaseprotoc; then
    return 0
  fi

  big_console_header "HBase protoc plugin: ${BUILDMODE}"

  start_clock

  personality_modules patch hbaseprotoc
  # Need to run 'install' instead of 'compile' because shading plugin
  # is hooked-up to 'install'; else hbase-protocol-shaded is left with
  # half of its process done.
  modules_workers patch hbaseprotoc install -DskipTests -Pcompile-protobuf -X -DHBasePatchProcess

  # shellcheck disable=SC2153
  until [[ $i -eq "${#MODULE[@]}" ]]; do
    if [[ ${MODULE_STATUS[${i}]} == -1 ]]; then
      ((result=result+1))
      ((i=i+1))
      continue
    fi
    module=${MODULE[$i]}
    fn=$(module_file_fragment "${module}")
    logfile="${PATCH_DIR}/patch-hbaseprotoc-${fn}.txt"

    count=$(${GREP} -c '\[ERROR\]' "${logfile}")

    if [[ ${count} -gt 0 ]]; then
      module_status ${i} -1 "patch-hbaseprotoc-${fn}.txt" "Patch generated "\
        "${count} new protoc errors in ${module}."
      ((result=result+1))
    fi
    ((i=i+1))
  done

  modules_messages patch hbaseprotoc true
  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

######################################

add_test_type hbaseanti

## @description  hbaseanti file filter
## @audience     private
## @stability    evolving
## @param        filename
function hbaseanti_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.java$ ]]; then
    add_test hbaseanti
  fi
}

## @description  hbaseanti patch file check
## @audience     private
## @stability    evolving
## @param        filename
function hbaseanti_patchfile
{
  local patchfile=$1
  local warnings
  local result

  if [[ "${BUILDMODE}" = full ]]; then
    return 0
  fi

  if ! verify_needed_test hbaseanti; then
    return 0
  fi

  big_console_header "Checking for known anti-patterns"

  start_clock

  warnings=$(${GREP} 'new TreeMap<byte.*()' "${patchfile}")
  if [[ ${warnings} -gt 0 ]]; then
    add_vote_table -1 hbaseanti "" "The patch appears to have anti-pattern where BYTES_COMPARATOR was omitted: ${warnings}."
    ((result=result+1))
  fi

  warnings=$(${GREP} 'import org.apache.hadoop.classification' "${patchfile}")
  if [[ ${warnings} -gt 0 ]]; then
    add_vote_table -1 hbaseanti "" "The patch appears use Hadoop classification instead of HBase: ${warnings}."
    ((result=result+1))
  fi

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi

  add_vote_table +1 hbaseanti "" "Patch does not have any anti-patterns."
  return 0
}


## @description  hbase custom mvnsite file filter.  See HBASE-15042
## @audience     private
## @stability    evolving
## @param        filename
function mvnsite_filefilter
{
  local filename=$1

  if [[ ${BUILDTOOL} = maven ]]; then
    if [[ ${filename} =~ src/main/site || ${filename} =~ src/main/asciidoc ]]; then
      yetus_debug "tests/mvnsite: ${filename}"
      add_test mvnsite
    fi
  fi
}
