#!/bin/bash

set -e

currentDir=$(pwd)
tmpDir=$(mktemp -d)
sourceURL=$(curl -sSL https://api.github.com/repos/onsi/ginkgo/releases/latest | jq -r .tarball_url)

echo "Updating to $sourceURL"
curl -sSL $sourceURL | tar -xz --strip-components=1 -C $tmpDir

# Remove tests
find $tmpDir -name '*_test.go' -delete

# Remove extra files
( cd $tmpDir; rm -r \
  CHANGELOG.md \
  CONTRIBUTING.md \
  ginkgo/generators/bootstrap_command.go \
  ginkgo/generators/generate_command.go \
  ginkgo/labels \
  ginkgo/outline \
  integration \
  internal/output_interceptor_unix.go \
  internal/output_interceptor_win.go \
  internal/test_helpers \
  go.mod \
  go.sum \
  .github \
  README.md \
  RELEASING.md )

# Rename module
find $tmpDir -type f -name '*.go' -exec sed -i 's/"github.com\/onsi/"github.com\/bsm/g' {} \;

# Copy files
cp -r $tmpDir/* .

# Apply patch
git apply < update.patch

# Tidy
go mod tidy

# Clean-up
rm -rf $tmpDir
