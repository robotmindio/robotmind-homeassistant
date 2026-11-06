# Fiber source governance

`robotmindio/robotmind-homeassistant` owns its source and native CI. The trusted contract is
`.github/pr-policy.json` and root `CODEOWNERS` on `main`. Ordinary changes
require all selected native workflows plus Fiber's independent review/verdict.
Governance and deployment authority require the named owner's exact-head approval.
A PR cannot bootstrap or relax its own trusted policy.

The repository administrator reviews and merges this initial trust root.
The central company declaration then supplies native check names and GitHub App
issuers to RobotMind's existing repository-governance reconciler. It preserves
stricter native protections, requires resolved conversations and owner review,
and disallows force pushes/deletion. Fiber's GitHub App must have the existing
contents, pull-request, workflow, checks and Actions capabilities on this repo.
See https://github.com/robotmindio/robotmind/blob/main/docs/gitops-multirepository.md.

`delivery.mode=source` requires preservation and successful main-push CI under
both accepted contracts, observed twice. `source_accepted` attests source only;
image publication, runtime rollout, device/store delivery and real hardware
acceptance retain their existing owners and evidence requirements.

Required source workflows: `.github/workflows/ci.yml`.
Private CI uses the existing organization pools; public/fork source stays isolated.
