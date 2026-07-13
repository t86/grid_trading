# ARX Frozen Inventory Cap 800 Implementation Plan

**Goal:** Keep the ARX best-quote frozen inventory cap at 800 USDT on hosts 114 and 150 after the recovery guard runs.

**Scope:** Change the ARX-specific recovery-guard policy, keep the ARX maintenance helper consistent, and enforce the existing total frozen cap at the transfer boundary. Runtime soft/hard inventory thresholds remain configured at 600/800 through each host's control file.

## Tasks

1. Update the ARX policy tests to require total, long, and short frozen caps of 800 USDT; run the focused tests and confirm they fail against the old constants.
2. Change the three ARX guard constants from 400/200/200 to 800/800/800.
3. Add runtime regressions proving a near-full total pool clamps a single transfer and is rechecked between two same-cycle sides; enforce the total cap at the transfer boundary.
4. Run the focused tests and the complete recovery-guard and reduce-freeze cap test files.
5. Commit and push the verified change to `origin/main`.
6. Update hosts 114 and 150 through their pull-based wrappers, restart ARX, and verify the control values remain 800 after at least one guard timer cycle.
