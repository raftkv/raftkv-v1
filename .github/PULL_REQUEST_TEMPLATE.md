## Description

Brief description of the changes in this PR.

## Type of Change

- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update
- [ ] Refactor / cleanup

## Testing

- [ ] `go build ./...` passes
- [ ] `go vet ./...` passes
- [ ] `go test . -skip TestRealGRPCConnectivity -count=1` passes
- [ ] New unit tests added for new functionality
- [ ] Regression gate (10 lines) still passes

## Checklist

- [ ] Code follows the project's style guidelines (`gofmt` compliant)
- [ ] No hardcoded secrets, keys, or credentials (R7 red line)
- [ ] Self-review completed
- [ ] Comments added only where necessary (prefer self-documenting code)
- [ ] CHANGELOG.md updated if applicable
- [ ] No breaking API changes (or CHANGELOG clearly documents them for 0.x releases)