#!/bin/bash

set -e

DRY_RUN=1

helps() {
    cat <<- EOF
Usage: $0 TAGVERSION [-t]

Creates git tags for public Go packages.

ARGUMENTS:
  TAGVERSION    Tag version to create, for example v1.0.0

OPTIONS:
  -t           Execute git commands (default: dry run)
EOF
    exit 0
}


if [ $# -eq 0 ]; then
    echo "Error: Tag version is required"
    helps
fi

TAG=$1
shift

while getopts "t" opt; do
    case $opt in
        t)
            DRY_RUN=0
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            exit 1
            ;;
    esac
done


if [ "$DRY_RUN" -eq 1 ]; then
    echo "Running in dry-run mode"
fi

if ! grep -Fq "\"${TAG#v}\"" version.go
then
    printf "version.go does not contain ${TAG#v}\n"
    exit 1
fi

GOMOD_ERRORS=0

# Check go.mod files for correct dependency versions
while read -r mod_file; do
    # Look for go-redis packages in require statements
    while read -r pkg version; do
        if [ "$version" != "${TAG}" ]; then
            printf "Error: %s has incorrect version for package %s: %s (expected %s)\n" "$mod_file" "$pkg" "$version" "${TAG}"
            GOMOD_ERRORS=$((GOMOD_ERRORS + 1))
        fi
    done < <(awk '/^require|^require \(/{p=1;next} /^\)/{p=0} p{if($1 ~ /^github\.com\/redis\/go-redis/){print $1, $2}}' "$mod_file")
done < <(find . -type f -name 'go.mod')

# Exit if there are gomod errors
if [ $GOMOD_ERRORS -gt 0 ]; then
    exit 1
fi


PACKAGE_DIRS=$(find . -mindepth 2 -type f -name 'go.mod' -exec dirname {} \; \
  | grep -E -v "example|internal" \
  | sed 's/^\.\///' \
  | sort)


execute_git_command() {
    if [ "$DRY_RUN" -eq 0 ]; then
        "$@"
    else
        echo "DRY-RUN: Would execute: $@"
    fi
}

execute_git_command git tag ${TAG}
execute_git_command git push origin ${TAG}

for dir in $PACKAGE_DIRS
do
    printf "tagging ${dir}/${TAG}\n"
    execute_git_command git tag ${dir}/${TAG}
    execute_git_command git push origin ${dir}/${TAG}
done
