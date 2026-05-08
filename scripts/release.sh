#!/bin/bash

set -e

help() {
    cat <<- EOF
Usage: TAG=tag $0

Updates redis/go-redis dependency versions in all sub-module go.mod files
and bumps version.go to the new tag. Runs in the current branch and does
NOT commit, push, or switch branches — review the diff and commit yourself.

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

# Portable in-place sed: works on both GNU sed and BSD/macOS sed by
# always writing a .bak backup and removing it afterwards.
sed_inplace() {
    local script=$1
    local file=$2
    sed -i.bak "$script" "$file"
    rm -f "${file}.bak"
}

PACKAGE_DIRS=$(find . -mindepth 2 -type f -name 'go.mod' -exec dirname {} \; \
  | sed 's/^\.\///' \
  | sort)

for dir in $PACKAGE_DIRS
do
    printf "updating %s/go.mod\n" "${dir}"
    sed_inplace \
        "s|\(github.com/redis/go-redis[^ ]*\) v[^ ]*|\1 ${TAG}|" \
        "${dir}/go.mod"
    (cd "./${dir}" && go mod tidy -compat=1.24)
done

printf "updating version.go\n"
sed_inplace "s/\(return \)\"[^\"]*\"/\1\"${TAG#v}\"/" ./version.go

printf "\nDone. Review changes with 'git diff' and commit when ready.\n"
