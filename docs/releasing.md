# Release process

## Preconditions

1. Open a release issue with the target version and checklist.
2. Confirm CI is green on every supported Spark line.
3. Prepare the version, `CHANGELOG.md`, and `docs/releases/<version>.md` in a
   reviewed pull request. Keep the alpha warning for alpha releases.
4. Review dependency, license, and security reports.
5. Confirm the PyPI project has a Trusted Publisher for this GitHub repository and
   the `pypi` environment.
6. Confirm clean wheel and sdist smoke checks pass in both core-only and supported
   Spark environments. Neither check may import Streamcase from the source tree.

## Candidate

Create the version change through a reviewed pull request and merge it to the
protected `main` branch before tagging. Every human-authored pull-request commit
requires a DCO `Signed-off-by` trailer. That trailer certifies contribution
rights; it is **not** a cryptographic signature. For the first alpha,
cryptographic commit and tag signatures are not required, and the release
workflow does not claim to verify them. This policy can be strengthened in a
separate issue before later releases.

Create or select the release tag only after the version pull request is merged.
Use the reviewed candidate notes as the basis for the draft GitHub release;
replace their candidate status and the changelog's pending date only after
publication and verification, through a separate reviewed change.
Tags use `vMAJOR.MINOR.PATCH` or a valid prerelease suffix. Before building any
distribution, the release workflow checks that the event tag still resolves to
the event's checked-out commit, that this commit is in the fetched `main`
history, and that the tag is exactly `v` followed by the package version. A
missing, moved, off-main, or version-mismatched tag stops the build before
publication. Main-branch ancestry relies on the repository's branch protection
and PR review policy; it is not proof of an individual review by itself.

## Artifact path

The GitHub release is a tag and release-notes page, **not** a wheel or sdist
download location. It must not promise attached distribution assets. The release
workflow builds one wheel and one sdist from the verified tag, validates them
with Twine, records their SHA-256 hashes in `SHA256SUMS`, and transfers all three
files in a temporary `python-distributions` **Actions artifact**. The protected
`pypi` job downloads that artifact and rejects missing, extra, or changed files
before moving `SHA256SUMS` out of `dist/`. Only the verified wheel and sdist are
then published to PyPI through OpenID Connect. No workstation upload or rebuild
occurs between verification and publication.

Actions artifacts expire according to repository retention settings and are not
GitHub release assets or a permanent distribution mirror. PyPI is the durable
distribution location. This flow also works when GitHub release immutability is
enabled: published immutable releases cannot gain or replace assets later. If
GitHub release attachments become a requirement, design and test a separate
draft-time attachment flow before changing this policy.

## Verification

- Confirm CI installed the wheel and sdist separately in clean environments,
  verified `streamcase.__version__`, and ran the supported Spark smoke example.
- Compare the wheel and sdist filenames and SHA-256 hashes in the workflow's
  `SHA256SUMS` output with `urls[].filename` and `urls[].digests.sha256` from
  `https://pypi.org/pypi/streamcase/<version>/json`. Record the comparison in
  the release issue. The Actions artifact's archive digest is not a substitute
  for checking the two individual distribution files.
- Announce the release only after verification succeeds.

If the transfer hash check fails, the publish job stops before the PyPI action.
If PyPI's published filenames or hashes do not match, do not announce the
release or attempt to overwrite files; record the discrepancy and investigate.
Never upload release artifacts manually from a developer workstation.
