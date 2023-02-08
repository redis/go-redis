#!/bin/bash

set -e

currentDir=$(pwd)
tmpDir=$(mktemp -d)
sourceURL=$(curl -sSL https://api.github.com/repos/onsi/gomega/releases/latest | jq -r .tarball_url)

echo "Updating to $sourceURL"
curl -sSL $sourceURL | tar -xz --strip-components=1 -C $tmpDir

# Remove tests
find $tmpDir -name '*_test.go' -delete

# Remove extra files
( cd $tmpDir; rm -rf \
  CHANGELOG.md \
  CONTRIBUTING.md \
  docker-compose.yaml \
  Dockerfile \
  gexec/_fixture \
  ghttp/protobuf \
  go.mod \
  go.sum \
  .github \
  Makefile \
  matchers/test_data \
  matchers/match_yaml_matcher.go \
  README.md \
  RELEASING.md \
  .travis.yml )

# Rename module
find $tmpDir -type f -name '*.go' -exec sed -i 's/"github.com\/onsi/"github.com\/bsm/g' {} \;

# Copy files
cp -r $tmpDir/* .

# Apply patch
# git apply < update.patch

# Tidy
# go mod tidy

# Clean-up
rm -rf $tmpDir
