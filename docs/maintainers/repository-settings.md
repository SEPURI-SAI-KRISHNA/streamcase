# Repository settings baseline

This guide records the intended GitHub repository metadata and administration
settings for Streamcase. Repository settings affect contribution and release
workflows but cannot be changed by merging a file alone. Maintainers review a
settings change through an issue and documentation pull request, apply it after
merge, and record the administrative action on the issue.

## Metadata and discovery

| Setting | Baseline | Rationale |
| --- | --- | --- |
| Description | `Deterministic testing for Apache Spark Structured Streaming pipelines` | States the current project scope without claiming unsupported backends or a published release. |
| Homepage | Unset | Add a URL only when a maintained documentation site or published package page exists. |
| Default branch | `main` | Matches contribution documentation, workflows, and branch protection. |

Use these repository topics:

- `apache-spark`
- `data-engineering`
- `pyspark`
- `pytest`
- `python`
- `stream-processing`
- `structured-streaming`
- `testing`

Topics describe the implemented project direction and improve discovery without
making release or compatibility guarantees.

## Repository features

| Feature | Baseline | Rationale |
| --- | --- | --- |
| Issues | Enabled | Tracks scoped defects, enhancements, maintenance, and releases. |
| Discussions | Enabled | Hosts usage questions and design conversations before implementation scope is agreed. |
| Wiki | Disabled | Project documentation remains versioned and reviewed in the repository. |
| Projects | Disabled | Enable a project board only when a maintainer owns and actively updates it. |

Disabling an unused feature avoids parallel, stale sources of project truth. A
future issue may enable a feature with an owner, maintenance expectations, and a
clear relationship to the roadmap.

## Merge strategy

| Setting | Baseline | Rationale |
| --- | --- | --- |
| Rebase merge | Enabled | Preserves contributor commits and their DCO trailers while keeping linear history. |
| Squash merge | Disabled | Replacing reviewed commits with a generated squash commit can obscure per-commit sign-off history. |
| Merge commits | Disabled | The protected branch requires linear history. |
| Delete head branches | Enabled | Removes merged remote branches and keeps the branch list current. |
| Auto-merge | Disabled | The sole maintainer performs the explicit self-review documented in `CONTRIBUTING.md` before merging. |

Pull requests remain mandatory. A merge-method setting does not bypass required
checks, conversation resolution, ownership, sign-off, or review policy.

## Protected default branch

The `main` branch remains protected with:

- pull requests required before changes enter the branch;
- required checks evaluated against the latest `main`;
- package quality, supported-Python, dependency-review, and CodeQL checks;
- linear history and resolved review conversations;
- enforcement for administrators; and
- force pushes and branch deletion disabled.

While Streamcase has one maintainer, the required approval count remains zero and
the documented self-review process applies. Follow
[CONTRIBUTING.md](../../CONTRIBUTING.md) when a second active maintainer makes
independent approval and required code-owner review enforceable without
deadlocking development.

This baseline does not change the names or number of required checks. Workflow or
branch-protection changes require their own issue and review.

## Sign-off status

Contributors use DCO trailers as described in `CONTRIBUTING.md`. GitHub's web
commit sign-off setting and repository-wide DCO enforcement are handled by the
dedicated DCO workstream. They must not be represented as enforced until that
work is reviewed, enabled, and verified.

## Administrative change procedure

1. Open an issue describing the current state, proposed settings, and rollback.
2. Update this guide through a focused pull request.
3. Merge only after required checks and the applicable review policy pass.
4. Apply the settings through GitHub administration or an auditable CLI command.
5. Read the repository configuration back and compare it with this baseline.
6. Record the result on the issue before closing it.

Never combine a visibility, ownership, default-branch, or security-policy change
with routine metadata maintenance. Those changes require dedicated risk review.
