# Release process

## Preconditions

1. Open a release issue with the target version and checklist.
2. Confirm CI is green on every supported Spark line.
3. Update `CHANGELOG.md` and remove the alpha warning when appropriate.
4. Review dependency, license, and security reports.
5. Confirm the PyPI project has a Trusted Publisher for this GitHub repository and
   the `pypi` environment.

## Candidate

Create the version change through a reviewed pull request and merge it to the
protected `main` branch before tagging. Every human-authored pull-request commit
requires a DCO `Signed-off-by` trailer. That trailer certifies contribution
rights; it is **not** a cryptographic signature. For the first alpha,
cryptographic commit and tag signatures are not required, and the release
workflow does not claim to verify them. This policy can be strengthened in a
separate issue before later releases.

Create or select the release tag only after the version pull request is merged.
Tags use `vMAJOR.MINOR.PATCH` or a valid prerelease suffix. Before building any
distribution, the release workflow checks that the event tag still resolves to
the event's checked-out commit, that this commit is in the fetched `main`
history, and that the tag is exactly `v` followed by the package version. A
missing, moved, off-main, or version-mismatched tag stops the build before
publication. Main-branch ancestry relies on the repository's branch protection
and PR review policy; it is not proof of an individual review by itself.

The GitHub release triggers the release workflow. CI builds the source and wheel
artifacts once, validates them with Twine, stores them on the GitHub release, and
publishes the exact same files to PyPI through OpenID Connect.

## Verification

- Install the wheel into a clean environment.
- Import `streamcase` and verify `streamcase.__version__`.
- Run the README example against a supported Spark version.
- Confirm PyPI metadata and artifact hashes.
- Announce the release only after verification succeeds.

Never upload release artifacts manually from a developer workstation.
