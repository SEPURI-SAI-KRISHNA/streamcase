# Contributing to Streamcase

Streamcase uses an issue-first, review-first workflow inspired by established
Apache projects. The project is not an Apache Software Foundation project.

## Before writing code

1. Search existing issues and pull requests.
2. Open an issue describing the problem, proposed behavior, and acceptance
   criteria.
3. Wait for scope agreement when the change affects public APIs, compatibility,
   security, or architecture.
4. Create a branch containing the issue number, such as
   `feat/123-watermark-actions` or `fix/456-progress-parsing`.

Direct code commits to `main` are not accepted.

Maintainers classify incoming work using the
[issue triage and label guide](docs/maintainers/triage.md). Labels communicate
scope and state, but do not replace acceptance criteria or review.

## Development setup

Create and activate a virtual environment, then install development dependencies:

```shell
python -m venv .venv
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Run the local quality gates:

```shell
ruff format --check .
ruff check .
mypy
pytest
python -m build
python -m twine check dist/*
```

Use `ruff format .` to apply formatting locally.

## Pull requests

- Link the issue with `Closes #123` in the pull-request description.
- Keep the pull request focused on one problem.
- Add or update tests for observable behavior.
- Update user documentation and the changelog when appropriate.
- Preserve compatibility unless the issue explicitly approves a breaking change.
- Resolve every review conversation before merge.
- Do not force-push after review unless necessary; explain rewritten history.

At least one approving review and all required CI checks are expected before
merge. The author does not approve their own pull request.

## Commit sign-off

Contributors certify the Developer Certificate of Origin 1.1 by signing off each
commit:

```shell
git commit -s -m "feat: add watermark action"
```

The sign-off asserts that you have the right to submit the contribution under the
project license. Read the DCO at <https://developercertificate.org/>.

## Commit messages

Use a concise Conventional Commit style where practical:

- `feat: add checkpoint restart action`
- `fix: preserve duplicate output rows`
- `docs: explain Spark compatibility`
- `test: cover state-row assertion`
- `ci: validate Spark 4.x`

## Compatibility changes

Changes to supported Python or Spark versions require:

- a dedicated issue;
- CI evidence;
- a changelog entry;
- documentation updates; and
- maintainer approval.

## Reporting security issues

Follow [SECURITY.md](SECURITY.md). Never include an unpatched vulnerability in a
public issue or pull request.
