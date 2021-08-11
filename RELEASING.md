# Releasing

1. Run `release.sh` script which updates versions in go.mod files and pushes a new branch to GitHub:

```shell
./scripts/release.sh -t v1.0.0
```

2. Open a pull request and wait for the build to finish.

3. Merge the pull request and run `tag.sh` to create tags for packages:

```shell
./scripts/tag.sh -t v1.0.0
```

4. Push the tags:

```shell
git push origin --tags
```
