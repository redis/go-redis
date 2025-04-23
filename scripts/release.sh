#!/bin/bash

set -e

help() {
    cat <<- EOF
Usage: TAG=tag $0

Updates version in go.mod files and pushes a new brash to GitHub.

VARIABLES:
  TAG        git tag, for example, v1.0.0
EOF
    exit 0
}

if [ -z "$TAG" ]
then
    printf "TAG is required\n\n"
    help
fi

TAG_REGEX="^v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(\\-[0-9A-Za-z-]+(\\.[0-9A-Za-z-]+)*)?(\\+[0-9A-Za-z-]+(\\.[0-9A-Za-z-]+)*)?$"
if ! [[ "${TAG}" =~ ${TAG_REGEX} ]]; then
    printf "TAG is not valid: ${TAG}\n\n"
    exit 1
fi

TAG_FOUND=`git tag --list ${TAG}`
if [[ ${TAG_FOUND} = ${TAG} ]] ; then
    printf "tag ${TAG} already exists\n\n"
    exit 1
fi

if ! git diff --quiet
then
    printf "working tree is not clean\n\n"
    git status
    exit 1
fi

git checkout master

PACKAGE_DIRS=$(find . -mindepth 2 -type f -name 'go.mod' -exec dirname {} \; \
  | sed 's/^\.\///' \
  | sort)

for dir in $PACKAGE_DIRS
do
    printf "${dir}: go get -u && go mod tidy\n"
    #(cd ./${dir} && go get -u && go mod tidy -compat=1.18)
done

for dir in $PACKAGE_DIRS
do
    sed --in-place \
        "s/redis\/go-redis\([^ ]*\) v.*/redis\/go-redis\1 ${TAG}/" "${dir}/go.mod"
    #(cd ./${dir} && go get -u && go mod tidy -compat=1.18)
    (cd ./${dir} && go mod tidy -compat=1.18)
done

sed --in-place "s/\(return \)\"[^\"]*\"/\1\"${TAG#v}\"/" ./version.go

git checkout -b release/${TAG} master
git add -u
git commit -m "chore: release $TAG (release.sh)"
git push origin release/${TAG}
