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

All protected-branch checks must pass before merge. A pull request opened by a
contributor requires approval from a maintainer who is not the author.

While the project has only one maintainer, a maintainer-authored pull request
cannot receive genuine independent approval from that same person. It may merge
only after the maintainer:

1. links a scoped issue and reviews the complete pull-request diff;
2. confirms every required check passes;
3. resolves every review conversation and records material decisions publicly;
4. verifies that every commit carries the required DCO sign-off; and
5. observes any additional waiting period, including the seven-day public-comment
   period for governance changes.

This process is a documented self-review, not an approval. The
[CODEOWNERS](.github/CODEOWNERS) file records current responsibility and requests
review where GitHub can do so; it does not bypass branch protection or turn an
author's review into independent approval.

When the project has a second active maintainer who can review without
deadlocking development, branch protection should require at least one approving
review and code-owner review. Until then, the issue, pull request, protected
checks, conversation-resolution, and sign-off requirements remain mandatory.

## Commit sign-off

Contributors certify the Developer Certificate of Origin 1.1 by signing off each
commit:

```shell
git commit -s -m "feat: add watermark action"
```

The sign-off asserts that you have the right to submit the contribution under the
project license. Read the DCO at <https://developercertificate.org/>.

The `DCO sign-off` check validates every non-merge human commit in a pull
request. Each trailer must use the following form, and its name and email must
match the commit author's or committer's identity:

```text
Signed-off-by: Random J Developer <random@developer.example.org>
```

Amend an unsigned commit with `git commit --amend --signoff`. If more than one
commit is missing a sign-off, rewrite each affected commit with an interactive
rebase. Rewriting published commits changes their identifiers, so coordinate
with other contributors and explain any necessary force-push on the pull
request. A later remediation commit does not repair an earlier unsigned commit,
and maintainers cannot override a failed check.

Recognized dependency-update bots are exempt because they cannot certify the
DCO as a person. Repository owners, maintainers, and other human contributors
are not exempt. GitHub web-based commits must also include a sign-off.

A DCO sign-off is a contribution certification. It is not a cryptographic commit
signature, copyright assignment, or substitute for code review.

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
