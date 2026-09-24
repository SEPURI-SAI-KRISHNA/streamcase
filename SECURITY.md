# Security policy

## Supported versions

Until the first stable release, security fixes are made on the latest published
alpha version and the default branch only.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use GitHub's private
vulnerability reporting for this repository:

<https://github.com/SEPURI-SAI-KRISHNA/streamcase/security/advisories/new>

Include the affected version, reproduction steps, potential impact, and any known
mitigation. You should receive an acknowledgement within seven days. No response
timeline can be guaranteed while the project has a single maintainer.

## Scope

Particularly relevant reports include unsafe filesystem handling, credential
exposure, arbitrary code execution beyond the user-supplied pipeline, and
checkpoint corruption. Streamcase does not accept or mutate production
checkpoints by design.

