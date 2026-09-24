# Governance

## Principles

Streamcase is developed in public through transparent issues, pull requests, and
recorded decisions. Technical merit, respectful participation, and sustained
contribution determine influence.

## Roles

- **Users** run Streamcase and provide feedback.
- **Contributors** submit issues, documentation, tests, or code.
- **Committers** are trusted contributors who may merge reviewed changes.
- **Maintainers** set release policy, compatibility guarantees, and project
  direction.

The initial maintainer is Sepuri Sai Krishna. New committers and maintainers may
be nominated after sustained, constructive participation. The decision and its
rationale should be recorded publicly.

## Decisions

Routine changes are decided through pull-request review. Significant changes use
a design issue and seek consensus. Examples include:

- public API changes;
- supported Spark or Python version changes;
- new execution backends;
- security-sensitive filesystem behavior; and
- governance or licensing changes.

When consensus cannot be reached, maintainers may call a vote. A simple majority
of active maintainers decides ordinary matters. Removing a maintainer or changing
the license requires at least a two-thirds majority. With one maintainer, decisions
must still be documented publicly and cannot bypass CI or review policy.

## Releases

Releases are proposed and reviewed through a release issue. Artifacts are built by
CI and published with trusted identity. A release must not be uploaded manually
from a maintainer workstation.

## Changes to governance

Governance changes require a dedicated issue and pull request. The pull request
must remain open for at least seven days to allow community comment.

