# Issue triage and labels

This guide defines Streamcase's initial issue and pull-request label taxonomy.
Maintainers use labels to make the backlog searchable and to communicate what a
contributor can do next. Labels do not replace a clear title, acceptance criteria,
or a linked pull request.

Keep classification small and evidence-based. An issue normally has one kind
label, no more than one workflow-state label, and any number of relevant area,
automation, or impact labels. Priority labels are intentionally omitted until the
project has enough competing work to apply them consistently.

## Kind of work

Apply one kind label to describe the primary reason for the change.

| Label | Use |
| --- | --- |
| `bug` | Reproducible behavior that conflicts with documented or intended behavior. |
| `enhancement` | A new capability or an improvement to existing behavior. |
| `documentation` | Documentation-only additions or corrections. |
| `maintenance` | Repository, tooling, refactoring, or housekeeping work with no intended user-visible behavior change. |
| `performance` | Measurable runtime, memory, startup, or test-suite performance work. |
| `security` | Public hardening work or remediation that is already safe to discuss publicly. |
| `testing` | Test infrastructure or coverage work that is not part of a specific bug fix or feature. |
| `accessibility` | A barrier affecting people with disabilities. |

Never use a public issue or the `security` label for a suspected undisclosed
vulnerability. Follow [SECURITY.md](../../SECURITY.md) and use GitHub private
vulnerability reporting.

## Workflow state

Use no more than one workflow-state label at a time.

| Label | Meaning | Remove when |
| --- | --- | --- |
| `needs-triage` | A maintainer has not yet confirmed the problem, scope, and intake route. | Initial triage is complete. |
| `needs-decision` | Work is waiting for a documented product, API, compatibility, or architecture decision. | The decision is recorded and the issue can become `ready` or be closed. |
| `blocked` | A named external issue, prerequisite, or unavailable resource prevents progress. | The blocking condition is resolved. |
| `ready` | Scope and acceptance criteria are clear, and implementation may begin. This is not a priority promise. | A linked implementation pull request opens or the issue stops being actionable. |

A typical issue moves from `needs-triage` to `ready`. Use `needs-decision` or
`blocked` only when the issue body or a comment identifies the specific decision
or blocker. Once a linked implementation pull request is open, its state is
visible from GitHub and no workflow-state label is required.

## Project area

Apply every area label needed to help the appropriate maintainer or contributor
find the issue.

| Label | Use |
| --- | --- |
| `area: core` | Public scenario models and backend-independent behavior. |
| `area: spark` | PySpark integration, streaming execution, checkpoints, or Spark compatibility. |
| `area: pytest` | Pytest fixtures, plugin registration, or test-runner integration. |
| `area: assertions` | Result comparison, diagnostics, and assertion APIs. |
| `area: packaging` | Python package metadata, builds, distributions, or PyPI publishing. |
| `area: ci` | Continuous integration, GitHub Actions, or repository automation. |
| `area: docs` | Guides, examples, API reference, or documentation tooling. |

## Automation and compatibility

These labels identify automated dependency work or the ecosystem affected by a
change. They may accompany a kind and area label.

| Label | Use |
| --- | --- |
| `dependencies` | Dependency additions, removals, constraints, or automated updates. |
| `python` | Python runtime compatibility or Python-package dependency updates. |
| `github-actions` | GitHub Actions dependency updates or runner compatibility. |

## Change impact

| Label | Use |
| --- | --- |
| `breaking-change` | An approved change that requires users to update code or configuration. |
| `release` | Release preparation, publishing, verification, or follow-up work. |

Adding `breaking-change` does not approve the change. Breaking changes still
require the dedicated issue, compatibility evidence, documentation, changelog,
and maintainer approval described in [CONTRIBUTING.md](../../CONTRIBUTING.md).

## Contributor discovery

| Label | Use |
| --- | --- |
| `good first issue` | The scope is small, documented, and suitable for a first contribution. |
| `help wanted` | Maintainers welcome community implementation or investigation. |

Only add `good first issue` after the acceptance criteria and likely files are
clear. It may be combined with `help wanted`.

## Questions and resolutions

Usage and open-ended design questions normally belong in GitHub Discussions.
Use `question` only when an existing issue needs a concrete answer before it can
be classified.

| Label | Use |
| --- | --- |
| `duplicate` | Another issue already tracks the same problem or request. |
| `invalid` | The report cannot be acted on as filed and no better intake route applies. |
| `wontfix` | The project has deliberately declined the request, with rationale recorded. |

When closing with a resolution label, link the canonical issue or explain the
decision in a final comment.

## Triage checklist

1. Confirm that the report uses the correct public or private intake route.
2. Check for duplicates and link related issues or discussions.
3. Confirm the problem statement, scope, acceptance criteria, and non-goals.
4. Apply one kind label, one workflow-state label when needed, and relevant area
   or compatibility labels.
5. Record any decision or blocker in the issue instead of relying on a label
   alone.
6. Remove `needs-triage` before implementation begins.
