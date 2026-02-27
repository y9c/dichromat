# GitHub Actions Workflows

This directory contains GitHub Actions workflows for continuous integration and deployment.

## Workflows

### `ci.yml`

Runs on every push and pull request to `main`, `master`, and `develop` branches.

**Jobs:**

1. **lint**: Checks Python code quality
   - Runs `ruff` linter and formatter
   - Runs `mypy` type checker

2. **build-docker**: Builds the Docker image
   - Sets up Docker Buildx
   - Builds the image from Dockerfile
   - Tests basic commands inside the container
   - Uses GitHub Actions cache for faster builds

3. **test-pipeline**: Tests the pipeline functionality
   - Creates minimal test data
   - Validates Snakefile syntax
   - Runs dry-run test

4. **security-scan**: Scans for vulnerabilities
   - Uses Trivy to scan filesystem for security issues
   - Uploads results to GitHub Security tab

### `release.yml`

Runs when a version tag `v*` is pushed (e.g., `v0.2.0`).

**Jobs:**

1. **build-and-push**: Builds and publishes the Docker image
   - Updates VERSION file
   - Builds multi-platform Docker image
   - Pushes to GitHub Container Registry (ghcr.io)
   - Tags: `latest`, `v{major}`, `v{major}.{minor}`, `v{major}.{minor}.{patch}`
   - Creates GitHub Release with artifacts

## Required Secrets

No additional secrets are required. The workflows use:
- `GITHUB_TOKEN` (automatically provided)

## Manual Triggers

To manually trigger a workflow run, you can use the GitHub CLI:

```bash
# Trigger CI workflow
gh workflow run ci.yml

# Trigger release (creates a new tag)
gh release create v0.2.0 --generate-notes
```

## Troubleshooting

### Build failures

If the Docker build fails:
1. Check the Dockerfile syntax
2. Verify all build dependencies are available
3. Check the Actions logs for specific error messages

### Security scan failures

If Trivy finds vulnerabilities:
1. Review the Security tab in the repository
2. Update dependencies in Dockerfile if needed
3. Consider adding `.trivyignore` for false positives
