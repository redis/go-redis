# Release Notes Template for go-redis

This template provides a structured format for creating release notes for go-redis releases.

## Format Structure

```markdown
# X.Y.Z (YYYY-MM-DD)

## 🚀 Highlights

### [Category Name]
Brief description of the major feature/change with context and impact.
- Key points
- Performance metrics if applicable
- Links to documentation

### [Another Category]
...

## ✨ New Features

- Feature description ([#XXXX](https://github.com/redis/go-redis/pull/XXXX)) by [@username](https://github.com/username)
- ...

## 🐛 Bug Fixes

- Fix description ([#XXXX](https://github.com/redis/go-redis/pull/XXXX)) by [@username](https://github.com/username)
- ...

## ⚡ Performance

- Performance improvement description ([#XXXX](https://github.com/redis/go-redis/pull/XXXX)) by [@username](https://github.com/username)
- ...

## 🧪 Testing & Infrastructure

- Testing/CI improvement ([#XXXX](https://github.com/redis/go-redis/pull/XXXX)) by [@username](https://github.com/username)
- ...

## 👥 Contributors

We'd like to thank all the contributors who worked on this release!

[@username1](https://github.com/username1), [@username2](https://github.com/username2), ...

---

**Full Changelog**: https://github.com/redis/go-redis/compare/vX.Y-1.Z...vX.Y.Z
```

## Guidelines

### Highlights Section
The Highlights section should contain the **most important** user-facing changes. Common categories include:

- **Typed Errors** - Error handling improvements
- **New Commands** - New Redis commands support (especially for new Redis versions)
- **Search & Vector** - RediSearch and vector-related features
- **Connection Pool** - Pool improvements and performance
- **Metrics & Observability** - Monitoring and instrumentation
- **Breaking Changes** - Any breaking changes (should be prominent)

Each highlight should:
- Have a descriptive title
- Include context about why it matters
- Link to relevant PRs
- Include performance metrics if applicable

### New Features Section
- List all new features with PR links and contributor attribution
- Use descriptive text, not just PR titles
- Group related features together if it makes sense

### Bug Fixes Section
- Only include actual bug fixes
- Be specific about what was broken and how it's fixed
- Include issue links if the PR references an issue

### Performance Section
- Separate from New Features to highlight performance work
- Include metrics when available (e.g., "47-67% faster", "33% less memory")
- Explain the impact on users

### Testing & Infrastructure Section
- Include only important testing/CI changes
- **Exclude** dependency bumps (e.g., dependabot PRs for actions)
- **Exclude** minor CI tweaks unless they're significant
- Include major Redis version updates in CI

### What to Exclude
- Dependency bumps (dependabot PRs)
- Minor documentation typo fixes
- Internal refactoring that doesn't affect users
- Duplicate entries (same PR in multiple sections)
- `dependabot[bot]` from contributors list

### Formatting Rules
1. **PR Links**: Use `([#XXXX](https://github.com/redis/go-redis/pull/XXXX))` format
2. **Contributor Links**: Use `[@username](https://github.com/username)` format
3. **Issue Links**: Use `([#XXXX](https://github.com/redis/go-redis/issues/XXXX))` format
4. **Full Changelog**: Always include at the bottom with correct version comparison

### Getting PR Information
Use GitHub API to fetch PR details:
```bash
# Get recent merged PRs
gh pr list --state merged --limit 50 --json number,title,author,mergedAt,url
```

Or use the GitHub web interface to review merged PRs between releases.

### Example Workflow
1. Gather all merged PRs since last release
2. Categorize PRs by type (feature, bug fix, performance, etc.)
3. Identify the 3-5 most important changes for Highlights
4. Remove duplicates and dependency bumps
5. Add PR and contributor links
6. Review for clarity and completeness
7. Add Full Changelog link with correct version tags

## Example (v9.17.0)

See the v9.17.0 release notes in `RELEASE-NOTES.md` for a complete example following this template.

