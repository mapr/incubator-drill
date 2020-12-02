#!/usr/bin/env bash
timing:shortWait() {
  log:warn 'Called empty interface in interfaces.sh: timing:shortWait() {'
}

file:exists() { local path=$1
  log:warn 'Called empty interface in interfaces.sh: file:exists() { local path=$1'
}

file:doesNotExist() { local path=$1
  log:warn 'Called empty interface in interfaces.sh: file:doesNotExist() { local path=$1'
}

file:contains() { local filePath=$1; local soughtString=$2
  log:warn 'Called empty interface in interfaces.sh: file:contains() { local filePath=$1; local soughtString=$2'
}

file:doesNotContain() { local filePath=$1; local soughtString=$2
  log:warn 'Called empty interface in interfaces.sh: file:doesNotContain() { local filePath=$1; local soughtString=$2'
}

file:replaceStringInFileContent() { local filePath=$1; local stringToReplace=$2; local replaceWithThisString=$3
  log:warn 'Called empty interface in interfaces.sh: originalSignature'
}

file:createIfNotExists() { local filePath=$1
  log:warn 'Called empty interface in interfaces.sh: file:createIfNotExists() { local filePath=$1'
}

file:newTemporary() {
  log:warn 'Called empty interface in interfaces.sh: file:newTemporary() {'
}

file:name:withoutExtension() { local filePath=$1
  log:warn 'Called empty interface in interfaces.sh: file:name:withoutExtension() { local filePath=$1'
}

browser:openURL() { local urlToOpen=$1
  log:warn 'Called empty interface in interfaces.sh: browser:openURL() { local urlToOpen=$1'
}

jq:install() {
  log:warn 'Called empty interface in interfaces.sh: jq:install() {'
}

jq:mac:install() {
  log:warn 'Called empty interface in interfaces.sh: jq:mac:install() {'
}

jq:linux:yum:install() {
  log:warn 'Called empty interface in interfaces.sh: jq:linux:yum:install() {'
}

jq:linux:aptGet:install() {
  log:warn 'Called empty interface in interfaces.sh: jq:linux:aptGet:install() {'
}

dockerMachine:isHostDefined() { local machineID=${1:-defaultDockerHost}
  log:warn 'Called empty interface in interfaces.sh: dockerMachine:isHostDefined() { local machineID=${1:-defaultDockerHost}'
}

dockerMachine:createHost() { local machineID=${1:-defaultDockerHost}
  log:warn 'Called empty interface in interfaces.sh: dockerMachine:createHost() { local machineID=${1:-defaultDockerHost}'
}

dockerMachine:start() { local machineID=${1:-defaultDockerHost}
  log:warn 'Called empty interface in interfaces.sh: dockerMachine:start() { local machineID=${1:-defaultDockerHost}'
}

dockerMachine:stop() { local machineID=${1:-defaultDockerHost}
  log:warn 'Called empty interface in interfaces.sh: dockerMachine:stop() { local machineID=${1:-defaultDockerHost}'
}

dockerMachine:defaultDockerHost() {
  log:warn 'Called empty interface in interfaces.sh: dockerMachine:defaultDockerHost() {'
}

dockerMachine:use() { local machineID=${1:-defaultDockerHost}
  log:warn 'Called empty interface in interfaces.sh: dockerMachine:use() { local machineID=${1:-defaultDockerHost}'
}

dockerMachine:install() {
  log:warn 'Called empty interface in interfaces.sh: dockerMachine:install() {'
}

dockerMachine:isRunning() { local machineID=${1:-defaultDockerHost}
  log:warn 'Called empty interface in interfaces.sh: dockerMachine:isRunning() { local machineID=${1:-defaultDockerHost}'
}

dockerMachine:addDevopsCACert() { local machineID=${1:-defaultDockerHost}
  log:warn 'Called empty interface in interfaces.sh: dockerMachine:addDevopsCACert() { local machineID=${1:-defaultDockerHost}'
}

python:virtualenv:install() {
  log:warn 'Called empty interface in interfaces.sh: python:virtualenv:install() {'
}

python:virtualenv:generate() {
  log:warn 'Called empty interface in interfaces.sh: python:virtualenv:generate() {'
}

python:virtualenv:generatedEnvPath() {
  log:warn 'Called empty interface in interfaces.sh: python:virtualenv:generatedEnvPath() {'
}

python:virtualenv:activate() {
  log:warn 'Called empty interface in interfaces.sh: python:virtualenv:activate() {'
}

python:pip:installDependencies() {
  log:warn 'Called empty interface in interfaces.sh: python:pip:installDependencies() {'
}

python:pip:publishToDevOpsRepo() {
  log:warn 'Called empty interface in interfaces.sh: python:pip:publishToDevOpsRepo() {'
}

python:run() { args=$@
  log:warn 'Called empty interface in interfaces.sh: python:run() { args=$@'
}

python:docker:version() { export pythonVersionForDocker
  log:warn 'Called empty interface in interfaces.sh: python:docker:version() { export pythonVersionForDocker'
}

python:docker:run() { local args=$@
  log:warn 'Called empty interface in interfaces.sh: python:docker:run() { local args=$@'
}

python:pip:setCredentials() {
  log:warn 'Called empty interface in interfaces.sh: python:pip:setCredentials() {'
}

python:pip:configureRemoteRepo() {
  log:warn 'Called empty interface in interfaces.sh: python:pip:configureRemoteRepo() {'
}

python:install() {
  log:warn 'Called empty interface in interfaces.sh: python:install() {'
}

python:mac:install() {
  log:warn 'Called empty interface in interfaces.sh: python:mac:install() {'
}

python:linux:aptGet:install() {
  log:warn 'Called empty interface in interfaces.sh: python:linux:aptGet:install() {'
}

python:linux:yum:install() {
  log:warn 'Called empty interface in interfaces.sh: python:linux:yum:install() {'
}

python:cli() { local args=$@
  log:warn 'Called empty interface in interfaces.sh: python:cli() { local args=$@'
}

jira:issue:appearsInString() { local stringThatMightContainJIRAReference=$1
  log:warn 'Called empty interface in interfaces.sh: jira:issue:appearsInString() { local stringThatMightContainJIRAReference=$1'
}

selfhosting:mount() {
  log:warn 'Called empty interface in interfaces.sh: selfhosting:mount() {'
}

kubernetes:dashboard() {
  log:warn 'Called empty interface in interfaces.sh: kubernetes:dashboard() {'
}

jenkins:master:url() {
  log:warn 'Called empty interface in interfaces.sh: jenkins:master:url() {'
}

jenkins:pullRequestBuilder:shaToBuild() {
  log:warn 'Called empty interface in interfaces.sh: jenkins:pullRequestBuilder:shaToBuild() {'
}

jenkins:credential() {
  log:warn 'Called empty interface in interfaces.sh: jenkins:credential() {'
}

jenkins:host() {
  log:warn 'Called empty interface in interfaces.sh: jenkins:host() {'
}

jenkins:crumb() { local credential=$1
  log:warn 'Called empty interface in interfaces.sh: jenkins:crumb() { local credential=$1'
}

jenkins:pullRequestJob:create() { local credential=$1
  log:warn 'Called empty interface in interfaces.sh: jenkins:pullRequestJob:create() { local credential=$1'
}

jenkins:job:create() { local jobTemplatePath=$1; local jobName=$2; local credential=$3
  log:warn 'Called empty interface in interfaces.sh: jenkins:job:create() { local jobTemplatePath=$1; local jobName=$2; local credential=$3'
}

jenkins:job:urlWithCredentialForJobName() { local jobName=$1; local credential=$2
  log:warn 'Called empty interface in interfaces.sh: jenkins:job:urlWithCredentialForJobName() { local jobName=$1; local credential=$2'
}

jenkins:job:urlWithoutCredentialForJobName() { local jobName=$1
  log:warn 'Called empty interface in interfaces.sh: jenkins:job:urlWithoutCredentialForJobName() { local jobName=$1'
}

jenkins:mergeJob:create() { local credential=$1
  log:warn 'Called empty interface in interfaces.sh: jenkins:mergeJob:create() { local credential=$1'
}

jenkins:pullRequestJob:name() {
  log:warn 'Called empty interface in interfaces.sh: jenkins:pullRequestJob:name() {'
}

jenkins:mergeJob:name() {
  log:warn 'Called empty interface in interfaces.sh: jenkins:mergeJob:name() {'
}

jenkins:pullRequestJob:url() {
  log:warn 'Called empty interface in interfaces.sh: jenkins:pullRequestJob:url() {'
}

jenkins:mergeJob:url() {
  log:warn 'Called empty interface in interfaces.sh: jenkins:mergeJob:url() {'
}

jenkins:jobs:list() {
  log:warn 'Called empty interface in interfaces.sh: jenkins:jobs:list() {'
}

jenkins:isPresent() {
  log:warn 'Called empty interface in interfaces.sh: jenkins:isPresent() {'
}

jenkins:isAbsent() {
  log:warn 'Called empty interface in interfaces.sh: jenkins:isAbsent() {'
}

prompt:forConfirmation() { local promptText=$1
  log:warn 'Called empty interface in interfaces.sh: prompt:forConfirmation() { local promptText=$1'
}

prompt:forPassword() { local promptText=$1
  log:warn 'Called empty interface in interfaces.sh: prompt:forPassword() { local promptText=$1'
}

prompt:forValue() { local promptText=$1
  log:warn 'Called empty interface in interfaces.sh: prompt:forValue() { local promptText=$1'
}

virtualbox:install() {
  log:warn 'Called empty interface in interfaces.sh: virtualbox:install() {'
}

virtualboxExtension:brew:install() {
  log:warn 'Called empty interface in interfaces.sh: virtualboxExtension:brew:install() {'
}

brewCask:install() {
  log:warn 'Called empty interface in interfaces.sh: brewCask:install() {'
}

brewCask:tap:versions() {
  log:warn 'Called empty interface in interfaces.sh: brewCask:tap:versions() {'
}

dvm:install() {
  log:warn 'Called empty interface in interfaces.sh: dvm:install() {'
}

dvm:install() {
  log:warn 'Called empty interface in interfaces.sh: dvm:install() {'
}

dvm:loader:path() {
  log:warn 'Called empty interface in interfaces.sh: dvm:loader:path() {'
}

dvm:loader:run() {
  log:warn 'Called empty interface in interfaces.sh: dvm:loader:run() {'
}

dvm:makeAvailableInTerminal() {
  log:warn 'Called empty interface in interfaces.sh: dvm:makeAvailableInTerminal() {'
}

pipeline:install:shouldPerformGitOperations() { export shouldPerformGitOperationsDuringPipelineInstall
  log:warn 'Called empty interface in interfaces.sh: originalSignature'
}

pipeline:install() { export enableGit
  log:warn 'Called empty interface in interfaces.sh: pipeline:install() { export enableGit'
}

pipeline:install:interfaces() {
  log:warn 'Called empty interface in interfaces.sh: pipeline:install:interfaces() {'
}

pipeline:install:hooks() {
  log:warn 'Called empty interface in interfaces.sh: pipeline:install:hooks() {'
}

pipeline:install:selfExtractor() { export pipelineSelfExtractorPath
  log:warn 'Called empty interface in interfaces.sh: pipeline:install:selfExtractor() { export pipelineSelfExtractorPath'
}

pipeline:install:bashCompletion() {
  log:warn 'Called empty interface in interfaces.sh: pipeline:install:bashCompletion() {'
}

pipeline:install:branchWithInstallationChanges() {
  log:warn 'Called empty interface in interfaces.sh: pipeline:install:branchWithInstallationChanges() {'
}

pipeline:install:branchToEnable() {
  log:warn 'Called empty interface in interfaces.sh: pipeline:install:branchToEnable() {'
}

pipeline:onLoad() {
  log:warn 'Called empty interface in interfaces.sh: pipeline:onLoad() {'
}

dev:setup() {
  log:warn 'Called empty interface in interfaces.sh: dev:setup() {'
}

git:hooks:preCommit() {
  log:warn 'Called empty interface in interfaces.sh: git:hooks:preCommit() {'
}

git:pullRequest:onCodeChange() {
  log:warn 'Called empty interface in interfaces.sh: git:pullRequest:onCodeChange() {'
}

git:merge:onComplete() { local artifactoryCredentials=${artifactoryCredentials}
  log:warn 'Called empty interface in interfaces.sh: git:merge:onComplete() { local artifactoryCredentials=${artifactoryCredentials}'
}

operatingSystem:packageManager() {
  log:warn 'Called empty interface in interfaces.sh: operatingSystem:packageManager() {'
}

operatingSystem:isOsx() {
  log:warn 'Called empty interface in interfaces.sh: operatingSystem:isOsx() {'
}

operatingSystem:isLinux() {
  log:warn 'Called empty interface in interfaces.sh: operatingSystem:isLinux() {'
}

operatingSystem:isCentosOrRedHat() {
  log:warn 'Called empty interface in interfaces.sh: operatingSystem:isCentosOrRedHat() {'
}

operatingSystem:isUbuntu() {
  log:warn 'Called empty interface in interfaces.sh: operatingSystem:isUbuntu() {'
}

string:isEmpty() { local possiblyEmptyString=$1
  log:warn 'Called empty interface in interfaces.sh: string:isEmpty() { local possiblyEmptyString=$1'
}

string:notEmpty() { local possiblyEmptyString=$1
  log:warn 'Called empty interface in interfaces.sh: string:notEmpty() { local possiblyEmptyString=$1'
}

string:toLowercase() { local stringToConvert=$1
  log:warn 'Called empty interface in interfaces.sh: string:toLowercase() { local stringToConvert=$1'
}

string:substring() { local fullString=$1; local startIndex=$2; local endIndex=$3
  log:warn 'Called empty interface in interfaces.sh: string:substring() { local fullString=$1; local startIndex=$2; local endIndex=$3'
}

string:contains() { local stringToSearch=$1; local soughtString=$2
  log:warn 'Called empty interface in interfaces.sh: string:contains() { local stringToSearch=$1; local soughtString=$2'
}

string:matchesRegex() { local stringToSearch=$1; local regularExpression=$2
  log:warn 'Called empty interface in interfaces.sh: string:matchesRegex() { local stringToSearch=$1; local regularExpression=$2'
}

string:doesNotContain() { local stringToSearch=$1; local soughtString=$2
  log:warn 'Called empty interface in interfaces.sh: string:doesNotContain() { local stringToSearch=$1; local soughtString=$2'
}

strings:areEqual() { local s1=$1; local s2=$2
  log:warn 'Called empty interface in interfaces.sh: strings:areEqual() { local s1=$1; local s2=$2'
}

strings:areNotEqual() { local s1=$1; local s2=$2
  log:warn 'Called empty interface in interfaces.sh: strings:areNotEqual() { local s1=$1; local s2=$2'
}

strings:replace() { local originalString=$1; local stringToReplace=$2; local replaceWithThisString=$3
  log:warn 'Called empty interface in interfaces.sh: originalSignature'
}

string:isNotEmpty:AndTrue(){ local stringToCheck=$1
  log:warn 'Called empty interface in interfaces.sh: string:isNotEmpty:AndTrue(){ local stringToCheck=$1'
}

string:orDefault() { local potentiallyEmptyValue=$1; local stringToUseIfEmpty=$2
  log:warn 'Called empty interface in interfaces.sh: string:orDefault() { local potentiallyEmptyValue=$1; local stringToUseIfEmpty=$2'
}

directory:exists() { local path=$1
  log:warn 'Called empty interface in interfaces.sh: directory:exists() { local path=$1'
}

directory:doesNotExist() { local path=$1
  log:warn 'Called empty interface in interfaces.sh: directory:doesNotExist() { local path=$1'
}

directory:newTemporary() {
  log:warn 'Called empty interface in interfaces.sh: directory:newTemporary() {'
}

bashCompletion:path() {
  log:warn 'Called empty interface in interfaces.sh: bashCompletion:path() {'
}

bashCompletion:configDirectory() {
  log:warn 'Called empty interface in interfaces.sh: bashCompletion:configDirectory() {'
}

bashCompletion:install() {
  log:warn 'Called empty interface in interfaces.sh: bashCompletion:install() {'
}

helm:installHelmItself() {
  log:warn 'Called empty interface in interfaces.sh: helm:installHelmItself() {'
}

helm:init:clientOnly() {
  log:warn 'Called empty interface in interfaces.sh: helm:init:clientOnly() {'
}

helm:cli() { local args=$@
  log:warn 'Called empty interface in interfaces.sh: helm:cli() { local args=$@'
}

helm:chart:repoTest:publish() { local helmChartName=$1; local integrationTestDirPath=$2
  log:warn 'Called empty interface in interfaces.sh: helm:chart:repoTest:publish() { local helmChartName=$1; local integrationTestDirPath=$2'
}

kubectl:install() {
  log:warn 'Called empty interface in interfaces.sh: kubectl:install() {'
}

kubectl:bashCompletion:install() {
  log:warn 'Called empty interface in interfaces.sh: kubectl:bashCompletion:install() {'
}

helloMinikube:echoServer:start() {
  log:warn 'Called empty interface in interfaces.sh: helloMinikube:echoServer:start() {'
}

helloMinikube:echoServer:view() {
  log:warn 'Called empty interface in interfaces.sh: helloMinikube:echoServer:view() {'
}

user:homeDirectory() {
  log:warn 'Called empty interface in interfaces.sh: user:homeDirectory() {'
}

user:bashProfile:path() {
  log:warn 'Called empty interface in interfaces.sh: user:bashProfile:path() {'
}

user:bashProfile:appendIfKeywordNotPresent() { local stringToAppendIfNotPresent=$1;
  log:warn 'Called empty interface in interfaces.sh: user:bashProfile:appendIfKeywordNotPresent() { local stringToAppendIfNotPresent=$1;'
}

minikube:dashboard() {
  log:warn 'Called empty interface in interfaces.sh: minikube:dashboard() {'
}

minikube:install() {
  log:warn 'Called empty interface in interfaces.sh: minikube:install() {'
}

minikube:start() {
  log:warn 'Called empty interface in interfaces.sh: minikube:start() {'
}

minikube:stop() {
  log:warn 'Called empty interface in interfaces.sh: minikube:stop() {'
}

minikube:ip() {
  log:warn 'Called empty interface in interfaces.sh: minikube:ip() {'
}

git:repositoryName() {
  log:warn 'Called empty interface in interfaces.sh: git:repositoryName() {'
}

git:install() {
  log:warn 'Called empty interface in interfaces.sh: git:install() {'
}

git:verifyPWDIsGitRoot() {
  log:warn 'Called empty interface in interfaces.sh: git:verifyPWDIsGitRoot() {'
}

git:rootFolder() {
  log:warn 'Called empty interface in interfaces.sh: git:rootFolder() {'
}

git:verifyCleanWorkingTree() {
  log:warn 'Called empty interface in interfaces.sh: git:verifyCleanWorkingTree() {'
}

pipeline:isSelfExecuting() {
  log:warn 'Called empty interface in interfaces.sh: pipeline:isSelfExecuting() {'
}

git:hooks:postRewrite() { local operationType=$1
  log:warn 'Called empty interface in interfaces.sh: git:hooks:postRewrite() { local operationType=$1'
}

git:hooks:install() {
  log:warn 'Called empty interface in interfaces.sh: git:hooks:install() {'
}

git:hooks:commitMessage() { local commitMessageTempFilePath=$1
  log:warn 'Called empty interface in interfaces.sh: git:hooks:commitMessage() { local commitMessageTempFilePath=$1'
}

git:message:verify:unitTestClaim() { local message=$1
  log:warn 'Called empty interface in interfaces.sh: git:message:verify:unitTestClaim() { local message=$1'
}

git:message:lowercaseFirst80Chars() { local message=$1
  log:warn 'Called empty interface in interfaces.sh: git:message:lowercaseFirst80Chars() { local message=$1'
}

git:message:verify() { local message=$1
  log:warn 'Called empty interface in interfaces.sh: git:message:verify() { local message=$1'
}

git:headCommit:message() {
  log:warn 'Called empty interface in interfaces.sh: git:headCommit:message() {'
}

git:messageForSHA() { local gitSHA=$1
  log:warn 'Called empty interface in interfaces.sh: git:messageForSHA() { local gitSHA=$1'
}

git:remotes:urlFor() { local remoteName=$1
  log:warn 'Called empty interface in interfaces.sh: git:remotes:urlFor() { local remoteName=$1'
}

git:remotes:origin:httpURL() {
  log:warn 'Called empty interface in interfaces.sh: git:remotes:origin:httpURL() {'
}

git:remotes:upstream:sshURL() {
  log:warn 'Called empty interface in interfaces.sh: git:remotes:upstream:sshURL() {'
}

git:remotes:upstream:homePageURL() {
  log:warn 'Called empty interface in interfaces.sh: git:remotes:upstream:homePageURL() {'
}

git:branch:create() { local branchName=$1
  log:warn 'Called empty interface in interfaces.sh: git:branch:create() { local branchName=$1'
}

git:branch:current() {
  log:warn 'Called empty interface in interfaces.sh: git:branch:current() {'
}

git:githubUserId() {
  log:warn 'Called empty interface in interfaces.sh: git:githubUserId() {'
}

git:branch:markRestricted() { local branchName=$1
  log:warn 'Called empty interface in interfaces.sh: git:branch:markRestricted() { local branchName=$1'
}

git:amendAndPush() {
  log:warn 'Called empty interface in interfaces.sh: git:amendAndPush() {'
}

git:cli() { local args=$@
  log:warn 'Called empty interface in interfaces.sh: git:cli() { local args=$@'
}

pipeline:build() {
  log:warn 'Called empty interface in interfaces.sh: pipeline:build() {'
}

pipeline:update() {
  log:warn 'Called empty interface in interfaces.sh: pipeline:update() {'
}

package:scanForSnapshotPackagesInRPM() { local artifactoryUrl=$1
  log:warn 'Called empty interface in interfaces.sh: package:scanForSnapshotPackagesInRPM() { local artifactoryUrl=$1'
}

java:mac:install() {
  log:warn 'Called empty interface in interfaces.sh: java:mac:install() {'
}

java:mac:v7:uploadToArtifactory() {
  log:warn 'Called empty interface in interfaces.sh: java:mac:v7:uploadToArtifactory() {'
}

java:mac:v7:oracle:downloadURL() {
  log:warn 'Called empty interface in interfaces.sh: java:mac:v7:oracle:downloadURL() {'
}

java:mac:v7:artifactory:downloadURL() {
  log:warn 'Called empty interface in interfaces.sh: java:mac:v7:artifactory:downloadURL() {'
}

finder:showHiddenFiles() {
  log:warn 'Called empty interface in interfaces.sh: finder:showHiddenFiles() {'
}

log:colors:green() {
  log:warn 'Called empty interface in interfaces.sh: log:colors:green() {'
}

log:colors:red() {
  log:warn 'Called empty interface in interfaces.sh: log:colors:red() {'
}

log:colors:yellow() {
  log:warn 'Called empty interface in interfaces.sh: log:colors:yellow() {'
}

log:colors:orange() {
  log:warn 'Called empty interface in interfaces.sh: log:colors:orange() {'
}

log:colors:darkGrey() {
  log:warn 'Called empty interface in interfaces.sh: log:colors:darkGrey() {'
}

log:colors:lightGrey() {
  log:warn 'Called empty interface in interfaces.sh: log:colors:lightGrey() {'
}

log:colors:default() {
  log:warn 'Called empty interface in interfaces.sh: log:colors:default() {'
}

log:level:default() {
  log:warn 'Called empty interface in interfaces.sh: log:level:default() {'
}

log:level:get() { export logLevel
  log:warn 'Called empty interface in interfaces.sh: log:level:get() { export logLevel'
}

log:level:set() { local newLogLevel=$1; export logLevel
  log:warn 'Called empty interface in interfaces.sh: log:level:set() { local newLogLevel=$1; export logLevel'
}

log:debug() { local logMessage=$@
  log:warn 'Called empty interface in interfaces.sh: log:debug() { local logMessage=$@'
}

log:info() { local logMessage=$@
  log:warn 'Called empty interface in interfaces.sh: log:info() { local logMessage=$@'
}

log:warn() { local logMessage=$@
  log:warn 'Called empty interface in interfaces.sh: log:warn() { local logMessage=$@'
}

log:error() { local logMessage=$@
  log:warn 'Called empty interface in interfaces.sh: log:error() { local logMessage=$@'
}

log:notImplemented() {
  log:warn 'Called empty interface in interfaces.sh: log:notImplemented() {'
}

log:timestamp() {
  log:warn 'Called empty interface in interfaces.sh: log:timestamp() {'
}

log:errorAndExit() { local errorMessage=$1
  log:warn 'Called empty interface in interfaces.sh: log:errorAndExit() { local errorMessage=$1'
}

log:success() { local successMessage=$1
  log:warn 'Called empty interface in interfaces.sh: log:success() { local successMessage=$1'
}

xcode:install() {
  log:warn 'Called empty interface in interfaces.sh: xcode:install() {'
}

xcode:isInstalled() {
  log:warn 'Called empty interface in interfaces.sh: xcode:isInstalled() {'
}

pipeline:inDocker() { local command=$1; local args=${@:2}
  log:warn 'Called empty interface in interfaces.sh: pipeline:inDocker() { local command=$1; local args=${@:2}'
}

curl:install() {
  log:warn 'Called empty interface in interfaces.sh: curl:install() {'
}

curl:cli() { local args=$@
  log:warn 'Called empty interface in interfaces.sh: curl:cli() { local args=$@'
}

bugzilla:issue:appearsInString() { local message=$1
  log:warn 'Called empty interface in interfaces.sh: bugzilla:issue:appearsInString() { local message=$1'
}

pipeline:verifyStandardChecks() {
  log:warn 'Called empty interface in interfaces.sh: pipeline:verifyStandardChecks() {'
}

homebrew:install() {
  log:warn 'Called empty interface in interfaces.sh: homebrew:install() {'
}

homebrew:update() {
  log:warn 'Called empty interface in interfaces.sh: homebrew:update() {'
}

bash:isInteractiveTerminal() {
  log:warn 'Called empty interface in interfaces.sh: bash:isInteractiveTerminal() {'
}

nvm:installDirectory() {
  log:warn 'Called empty interface in interfaces.sh: nvm:installDirectory() {'
}

nvm:download:url() {
  log:warn 'Called empty interface in interfaces.sh: nvm:download:url() {'
}

nvm:install:nvmItself() {
  log:warn 'Called empty interface in interfaces.sh: nvm:install:nvmItself() {'
}

nvm:bashProfileContent() {
  log:warn 'Called empty interface in interfaces.sh: nvm:bashProfileContent() {'
}

nvm:loader:create() {
  log:warn 'Called empty interface in interfaces.sh: nvm:loader:create() {'
}

nvm:loader:path() {
  log:warn 'Called empty interface in interfaces.sh: nvm:loader:path() {'
}

nvm:loader:run() {
  log:warn 'Called empty interface in interfaces.sh: nvm:loader:run() {'
}

nvm:use() { #Alias for nvm:useNodeVersion
  log:warn 'Called empty interface in interfaces.sh: nvm:use() { #Alias for nvm:useNodeVersion'
}

nvm:current() {
  log:warn 'Called empty interface in interfaces.sh: nvm:current() {'
}

nvm:useNodeVersion() { local desiredNodeVersion=${1:-$(nvm:nodeVersionForProject)}
  log:warn 'Called empty interface in interfaces.sh: nvm:useNodeVersion() { local desiredNodeVersion=${1:-$(nvm:nodeVersionForProject)}'
}

nvm:uninstall() {
  log:warn 'Called empty interface in interfaces.sh: nvm:uninstall() {'
}

nvm:makeAvailableInTerminal() {
  log:warn 'Called empty interface in interfaces.sh: nvm:makeAvailableInTerminal() {'
}

nvm:cli() { local args=$@
  log:warn 'Called empty interface in interfaces.sh: nvm:cli() { local args=$@'
}

nvm:install() { local desiredNodeVersion=${1:-$(nvm:nodeVersionForProject)}
  log:warn 'Called empty interface in interfaces.sh: nvm:install() { local desiredNodeVersion=${1:-$(nvm:nodeVersionForProject)}'
}

nvm:isInstalled() {
  log:warn 'Called empty interface in interfaces.sh: nvm:isInstalled() {'
}

nvm:isAvailable() {
  log:warn 'Called empty interface in interfaces.sh: nvm:isAvailable() {'
}

nvm:rc:file() {
  log:warn 'Called empty interface in interfaces.sh: nvm:rc:file() {'
}

nvm:nodeVersionForProject() {
  log:warn 'Called empty interface in interfaces.sh: nvm:nodeVersionForProject() {'
}

node:cli() { local args=$@
  log:warn 'Called empty interface in interfaces.sh: node:cli() { local args=$@'
}

nodejs:version:actual() {
  log:warn 'Called empty interface in interfaces.sh: nodejs:version:actual() {'
}

nodejs:someVersionIsInstalled() {
  log:warn 'Called empty interface in interfaces.sh: nodejs:someVersionIsInstalled() {'
}

nodejs:version:expected() {
  log:warn 'Called empty interface in interfaces.sh: nodejs:version:expected() {'
}

nodejs:expectedVersionIsInstalled() {
  log:warn 'Called empty interface in interfaces.sh: nodejs:expectedVersionIsInstalled() {'
}

nodejs:install() {
  log:warn 'Called empty interface in interfaces.sh: nodejs:install() {'
}

nodejs:version:isUnpublished() {
  log:warn 'Called empty interface in interfaces.sh: nodejs:version:isUnpublished() {'
}

gsed:run() { local sedArgs=$@
  log:warn 'Called empty interface in interfaces.sh: gsed:run() { local sedArgs=$@'
}

sed:run() { local sedArgs=$@
  log:warn 'Called empty interface in interfaces.sh: sed:run() { local sedArgs=$@'
}

artifactory:baseURL() {
  log:warn 'Called empty interface in interfaces.sh: artifactory:baseURL() {'
}

artifactory:localFlatFiles:publish() { local localArtifactPath=$1; local remoteArtifactPath=$2;
  log:warn 'Called empty interface in interfaces.sh: artifactory:localFlatFiles:publish() { local localArtifactPath=$1; local remoteArtifactPath=$2;'
}

artifactory:localFlatFiles:validateArtifactDoesNotExist() { local remoteArtifactPath=$1; local messageToShowIfError=$2
  log:warn 'Called empty interface in interfaces.sh: originalSignature'
}

artifactory:localFlatFiles:artifactExists() { local remoteArtifactPath=$1;
  log:warn 'Called empty interface in interfaces.sh: artifactory:localFlatFiles:artifactExists() { local remoteArtifactPath=$1;'
}

artifactory:localFlatFiles:artifactDoesNotExist() { local remoteArtifactPath=$1;
  log:warn 'Called empty interface in interfaces.sh: artifactory:localFlatFiles:artifactDoesNotExist() { local remoteArtifactPath=$1;'
}

artifactory:helmCharts:url() {
  log:warn 'Called empty interface in interfaces.sh: artifactory:helmCharts:url() {'
}

artifactory:helmCharts:publish() { local localArtifactPath=$1; local remoteArtifactPath=$2;
  log:warn 'Called empty interface in interfaces.sh: artifactory:helmCharts:publish() { local localArtifactPath=$1; local remoteArtifactPath=$2;'
}

artifactory:credentials:prepareNpmRCFile() {
  log:warn 'Called empty interface in interfaces.sh: artifactory:credentials:prepareNpmRCFile() {'
}

certs:devopsRootCert:install() {
  log:warn 'Called empty interface in interfaces.sh: certs:devopsRootCert:install() {'
}

certs:devopsRootCert:isInstalled() {
  log:warn 'Called empty interface in interfaces.sh: certs:devopsRootCert:isInstalled() {'
}

certs:userCert:isInstalled() { local issuerEmail=$1
  log:warn 'Called empty interface in interfaces.sh: certs:userCert:isInstalled() { local issuerEmail=$1'
}

certs:devopsRootCert:name() {
  log:warn 'Called empty interface in interfaces.sh: certs:devopsRootCert:name() {'
}

certs:devopsRootCert:issuerEmail() {
  log:warn 'Called empty interface in interfaces.sh: certs:devopsRootCert:issuerEmail() {'
}

certs:onMac:userCert:isInstalled() { local issuerEmail=$1
  log:warn 'Called empty interface in interfaces.sh: certs:onMac:userCert:isInstalled() { local issuerEmail=$1'
}

certs:onMac:userKeychainPath() {
  log:warn 'Called empty interface in interfaces.sh: certs:onMac:userKeychainPath() {'
}

certs:onMac:systemKeychainPath() {
  log:warn 'Called empty interface in interfaces.sh: certs:onMac:systemKeychainPath() {'
}

certs:devopsRootCert:install:onMac() {
  log:warn 'Called empty interface in interfaces.sh: certs:devopsRootCert:install:onMac() {'
}

certs:devopsRootCert:install:onCentosOrRedHat() {
  log:warn 'Called empty interface in interfaces.sh: certs:devopsRootCert:install:onCentosOrRedHat() {'
}

certs:devopsRootCert:pemSourcePath() {
  log:warn 'Called empty interface in interfaces.sh: certs:devopsRootCert:pemSourcePath() {'
}

certs:devopsRootCert:install:onUbuntu() {
  log:warn 'Called empty interface in interfaces.sh: certs:devopsRootCert:install:onUbuntu() {'
}

reflection:currentFunctionName() {
  log:warn 'Called empty interface in interfaces.sh: reflection:currentFunctionName() {'
}

reflection:ancestorFunctionName() { local ancestorNumberWhere0IsCallingFunction=$1
  log:warn 'Called empty interface in interfaces.sh: reflection:ancestorFunctionName() { local ancestorNumberWhere0IsCallingFunction=$1'
}

reflection:sortedPublicFunctions() {
  log:warn 'Called empty interface in interfaces.sh: reflection:sortedPublicFunctions() {'
}

reflection:generateInterfaces() {
  log:warn 'Called empty interface in interfaces.sh: reflection:generateInterfaces() {'
}

reflection:interfaceBodyTemplate() {
  log:warn 'Called empty interface in interfaces.sh: reflection:interfaceBodyTemplate() {'
}

reflection:shouldInstall() { local programName=$1
  log:warn 'Called empty interface in interfaces.sh: reflection:shouldInstall() { local programName=$1'
}

reflection:programExists() { local programName=$1
  log:warn 'Called empty interface in interfaces.sh: reflection:programExists() { local programName=$1'
}

reflection:publicFunctionWithAbbreviation() {
  log:warn 'Called empty interface in interfaces.sh: reflection:publicFunctionWithAbbreviation() {'
}

reflection:bashSourcesDirectory:set() { local thePathToBashSourceRoot=$1
  log:warn 'Called empty interface in interfaces.sh: reflection:bashSourcesDirectory:set() { local thePathToBashSourceRoot=$1'
}

reflection:bashSourcesDirectory() {
  log:warn 'Called empty interface in interfaces.sh: reflection:bashSourcesDirectory() {'
}

reflection:interfacesFilePath() {
  log:warn 'Called empty interface in interfaces.sh: reflection:interfacesFilePath() {'
}

reflection:pipelineHooksFilePath() {
  log:warn 'Called empty interface in interfaces.sh: reflection:pipelineHooksFilePath() {'
}

reflection:pipelineRootFolder() {
  log:warn 'Called empty interface in interfaces.sh: reflection:pipelineRootFolder() {'
}

reflection:stackTrace() {
  log:warn 'Called empty interface in interfaces.sh: reflection:stackTrace() {'
}

reflection:pipelineExecutablePath() {
  log:warn 'Called empty interface in interfaces.sh: reflection:pipelineExecutablePath() {'
}

pipeline:version() {
  log:warn 'Called empty interface in interfaces.sh: pipeline:version() {'
}

mvn:install:mvnItself() {
  log:warn 'Called empty interface in interfaces.sh: mvn:install:mvnItself() {'
}

mvn:linux:yum:install:mvnItself() {
  log:warn 'Called empty interface in interfaces.sh: mvn:linux:yum:install:mvnItself() {'
}

mvn:linux:aptGet:install:mvnItself() {
  log:warn 'Called empty interface in interfaces.sh: mvn:linux:aptGet:install:mvnItself() {'
}

mvn:cli() { local args=$@
  log:warn 'Called empty interface in interfaces.sh: mvn:cli() { local args=$@'
}

mvn:install() { local args=$@
  log:warn 'Called empty interface in interfaces.sh: mvn:install() { local args=$@'
}

docker:install() {
  log:warn 'Called empty interface in interfaces.sh: docker:install() {'
}

docker:mac:install() {
  log:warn 'Called empty interface in interfaces.sh: docker:mac:install() {'
}

docker:linux:yum:install() {
  log:warn 'Called empty interface in interfaces.sh: docker:linux:yum:install() {'
}

docker:linux:aptGet:install() {
  log:warn 'Called empty interface in interfaces.sh: docker:linux:aptGet:install() {'
}

docker:hub:repo:legacy() {
  log:warn 'Called empty interface in interfaces.sh: docker:hub:repo:legacy() {'
}

docker:interactiveTerminal:forImage() { local imageName=$1
  log:warn 'Called empty interface in interfaces.sh: docker:interactiveTerminal:forImage() { local imageName=$1'
}

docker:interactiveTerminal:forRunningContainer() { local containerId=$1
  log:warn 'Called empty interface in interfaces.sh: docker:interactiveTerminal:forRunningContainer() { local containerId=$1'
}

docker:demon:defaultConfig() {
  log:warn 'Called empty interface in interfaces.sh: docker:demon:defaultConfig() {'
}

docker:exec() { local dockerArgs=$@
  log:warn 'Called empty interface in interfaces.sh: docker:exec() { local dockerArgs=$@'
}

docker:cli() { local args=$@
  log:warn 'Called empty interface in interfaces.sh: docker:cli() { local args=$@'
}

docker:run:with:remove() { local args=$@
  log:warn 'Called empty interface in interfaces.sh: docker:run:with:remove() { local args=$@'
}

docker:run() { local args=$@
  log:warn 'Called empty interface in interfaces.sh: docker:run() { local args=$@'
}

docker:build() { local args=$@
  log:warn 'Called empty interface in interfaces.sh: docker:build() { local args=$@'
}

docker:info() {
  log:warn 'Called empty interface in interfaces.sh: docker:info() {'
}

docker:push() { local args=$@
  log:warn 'Called empty interface in interfaces.sh: docker:push() { local args=$@'
}

docker:daemon:isRunning() {
  log:warn 'Called empty interface in interfaces.sh: docker:daemon:isRunning() {'
}

docker:daemon:start() {
  log:warn 'Called empty interface in interfaces.sh: docker:daemon:start() {'
}

docker:buildTagAndOptionallyPush() { local name=$1; local dockerfilePath=$2; local buildContextPath=$3; local optionalRepository=$4
  log:warn 'Called empty interface in interfaces.sh: originalSignature'
}

docker:buildTagAndPush() { local nameAndTag=$1; local dockerfilePath=$2; local buildContextPath=$3; local repository=$4
  log:warn 'Called empty interface in interfaces.sh: originalSignature'
}

docker:buildAndTag() { local nameAndTag=$1; local dockerfilePath=$2; local buildContextPath=$3;
  log:warn 'Called empty interface in interfaces.sh: docker:buildAndTag() { local nameAndTag=$1; local dockerfilePath=$2; local buildContextPath=$3;'
}

docker:build:tag:forLocalDev() {
  log:warn 'Called empty interface in interfaces.sh: docker:build:tag:forLocalDev() {'
}

docker:inK8s:repo() {
  log:warn 'Called empty interface in interfaces.sh: docker:inK8s:repo() {'
}

docker:repoTestDockerfile:buildAndPush:versionedAndLatest() { local imageName=$1; local imageVersionTag=$2; local dockerFileName=$3
  log:warn 'Called empty interface in interfaces.sh: originalSignature'
}

docker:repoTestDockerfile:run() { local repoName=$1; local dockerFileName=$2; local command=$3; local args=${@:4}
  log:warn 'Called empty interface in interfaces.sh: originalSignature'
}

docker:repoImage:nameAndUniqueTagForDev() { local repoName=$1
  log:warn 'Called empty interface in interfaces.sh: docker:repoImage:nameAndUniqueTagForDev() { local repoName=$1'
}

docker:tagAndPush() { local sourceImage=$1; local targetImage=$2
  log:warn 'Called empty interface in interfaces.sh: docker:tagAndPush() { local sourceImage=$1; local targetImage=$2'
}

docker:tagAndPush:usingID() { local sourceImage=$1; local sourceTag=$2; local targetImageNameAndTag=$3
  log:warn 'Called empty interface in interfaces.sh: originalSignature'
}

docker:clean:dangling:images() {
  log:warn 'Called empty interface in interfaces.sh: docker:clean:dangling:images() {'
}

docker:remove:exited:containers() {
  log:warn 'Called empty interface in interfaces.sh: docker:remove:exited:containers() {'
}

docker:remove:repo:images() { local repoName=$1
  log:warn 'Called empty interface in interfaces.sh: docker:remove:repo:images() { local repoName=$1'
}

npm:install() { local args=$@
  log:warn 'Called empty interface in interfaces.sh: npm:install() { local args=$@'
}

npm:cli() { local args=$@
  log:warn 'Called empty interface in interfaces.sh: npm:cli() { local args=$@'
}

npm:run() { local args=$@
  log:warn 'Called empty interface in interfaces.sh: npm:run() { local args=$@'
}

npm:test() { local args=$@
  log:warn 'Called empty interface in interfaces.sh: npm:test() { local args=$@'
}

npm:publish() {
  log:warn 'Called empty interface in interfaces.sh: npm:publish() {'
}

vagrant:install() {
  log:warn 'Called empty interface in interfaces.sh: vagrant:install() {'
}

vagrant:scp:install() {
  log:warn 'Called empty interface in interfaces.sh: vagrant:scp:install() {'
}

vagrant:osx:up() {
  log:warn 'Called empty interface in interfaces.sh: vagrant:osx:up() {'
}

vagrant:osx:ssh() {
  log:warn 'Called empty interface in interfaces.sh: vagrant:osx:ssh() {'
}

