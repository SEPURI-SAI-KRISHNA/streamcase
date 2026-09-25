# Release process

## Preconditions

1. Open a release issue with the target version and checklist.
2. Confirm CI is green on every supported Spark line.
3. Update `CHANGELOG.md` and remove the alpha warning when appropriate.
4. Review dependency, license, and security reports.
5. Confirm the PyPI project has a Trusted Publisher for this GitHub repository and
   the `pypi` environment.

## Candidate

Create a signed version commit and tag only after the release pull request is
approved and merged. Tags use `vMAJOR.MINOR.PATCH` or a valid prerelease suffix.
Before building any distribution, CI requires the GitHub release tag to equal
`v` followed by the package version exactly. A mismatch stops the release
workflow before publication.

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
