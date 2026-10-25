# Repository settings baseline

This guide describes the intended GitHub repository settings for Streamcase.
It is a policy, not proof that the live repository already matches it. Merging
a documentation pull request cannot change GitHub settings. Maintainers apply
the reviewed changes separately, read them back, and record the result on the
tracking issue. Issue [#18](https://github.com/SEPURI-SAI-KRISHNA/streamcase/issues/18)
tracks the first application of this baseline.

## Metadata and discovery

| Setting | Baseline | Reason |
| --- | --- | --- |
| Description | `Deterministic testing for Apache Spark Structured Streaming pipelines` | Names the implemented Spark scope without implying Flink support. |
| Homepage | `https://pypi.org/project/streamcase/` | The package is published; PyPI is a maintained installation entry point until a dedicated site exists. |
| Default branch | `main` | Matches the contribution workflow and protected-branch rule. |

Use the topics `apache-spark`, `data-engineering`, `pyspark`, `pytest`,
`python`, `stream-processing`, `structured-streaming`, and `testing`.
Topics aid discovery but do not expand the compatibility claim in the
[Spark support policy](../spark-compatibility.md).

## Repository features

| Feature | Baseline | Reason |
| --- | --- | --- |
| Issues | Enabled | Scoped work, defects, and releases have a public record. |
| Discussions | Enabled | Usage questions and early design conversations have a home. |
| Wiki | Disabled | Project documentation remains versioned and reviewed here. |
| Projects | Disabled | Enable a board only when a maintainer owns and updates it. |

Reopen a disabled surface only through a scoped issue that defines its owner,
purpose, and relationship to the versioned roadmap.

## Merge strategy

| Setting | Baseline | Reason |
| --- | --- | --- |
| Rebase merge | Enabled | Keeps linear history while retaining each reviewed commit and DCO trailer. |
| Squash merge | Disabled | A generated squash commit obscures the original per-commit sign-off history. |
| Merge commits | Disabled | `main` requires linear history. |
| Delete merged head branches | Enabled | Removes finished remote work branches. |
| Auto-merge | Disabled | The sole maintainer performs the explicit self-review before merging. |

Pull requests remain mandatory. Enabling rebase does not waive required checks,
conversation resolution, DCO sign-off, or the review policy in
[CONTRIBUTING.md](../../CONTRIBUTING.md). Do not change branch protection as
part of applying these repository metadata and feature settings.

## Protected `main` and sign-off

The active `main` ruleset requires pull requests, linear history, current
required checks, and resolved review conversations. It blocks branch deletion
and non-fast-forward updates, with no bypass actors. Required checks include
package quality, the Python 3.10-3.13 matrix, dependency review, CodeQL, and
`DCO sign-off`. GitHub web-commit sign-off is enabled. A DCO trailer certifies
contribution rights; it is not a cryptographic signature or independent review.

The required approval count is zero while Streamcase has one maintainer.
Maintainer-authored pull requests follow the documented self-review process;
this is not an approval. Other contributors still need a non-author maintainer
approval. When a second active maintainer makes independent review practical,
change the review requirements through their own issue and pull request.
Code ownership records responsibility but does not create an approval that a
single author can give themselves.

This baseline does not change the names or count of required checks, ruleset
bypass, approval count, release tags, or security policy. Changes to those
protections need separate review.

## Administrative application and verification

1. Review the settings change in a scoped issue and documentation pull
   request. Changes to `GOVERNANCE.md` observe its seven-day public-comment
   period.
2. After merge, apply only the approved metadata, topics, repository-feature,
   and merge-method values in GitHub administration.
3. Read the repository configuration back and compare every value above,
   including web sign-off and the unchanged default branch. Inspect the
   `main` ruleset separately to confirm that required protections remain.
4. Record the before/after values, verification result, and any exception on
   the issue. Close it only when the live state matches the approved baseline.

If an administrative setting causes a problem, restore its prior value, record
the rollback on the issue, and open a focused follow-up. Do not change
visibility, ownership, default branch, security controls, or branch protection
as an incidental part of this procedure.
