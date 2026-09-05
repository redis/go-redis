# Maintainer Evaluation Framework

Use this reference when a claim is ambiguous, severity is disputed, or a technically correct PR may not justify permanent maintenance.

## Contents

- [Decision model](#decision-model)
- [Severity](#severity)
- [Evidence strength](#evidence-strength)
- [Unmet need and alternative design gate](#unmet-need-and-alternative-design-gate)
- [Issue disposition](#issue-disposition)
- [PR quality and value](#pr-quality-and-value)
- [Documentation threshold](#documentation-threshold)
- [Lifecycle and failure paths](#lifecycle-and-failure-paths)
- [Concurrency and cleanup ownership](#concurrency-and-cleanup-ownership)
- [Better-alternative prompts](#better-alternative-prompts)
- [Competing pull requests](#competing-pull-requests)
- [Maintainer comments](#maintainer-comments)
- [Compact report variants](#compact-report-variants)

## Decision Model

Treat validity, severity, and merge-worthiness as separate outputs. Also distinguish a `Preliminary assessment`, which may still require approved runtime evidence, from a final `Maintainer decision`. Do not label a provisional positive result as a verdict or final decision.

| Dimension | Question | Strong evidence |
| --- | --- | --- |
| Claim validity | Does the exact behavior occur, and is the proposed cause correct? | Reproduction, failing focused test (`go test -run TestGinkgoSuite . -ginkgo.focus="..."` or `go test -run TestName ./internal/pool/...`), or complete reachable path |
| Reachability | Can supported realistic inputs reach it? | Exported-API trace, real `redis.Options`, user report, or release comparison against the current git semver tag / `Version()` |
| Consequence | What fails, and is it silent or recoverable? | Observed output/error/state and downstream effect |
| Breadth | Which client variants, protocols, execution modes, deployments (Redis CE vs Redis Enterprise), and Redis versions are affected? | Explicit path and a Client / ClusterClient / Ring / Sentinel-failover / UniversalClient × RESP2/RESP3 × pipeline/tx/pubsub × deployment (`RE_CLUSTER`) matrix, assessed against the README supported-versions policy |
| Frequency | Is it normal, intermittent, or pathological? | Repeats, deterministic preconditions, reports, or telemetry |
| Need evidence | Is the exact scope demonstrated, merely plausible, already covered, or unsupported? | Same-scope user scenario, real-path reproduction, released compatibility requirement, violated supported contract, repeated demand, or broad consequential invariant |
| Unmet need | What user outcome cannot be achieved through supported behavior today, or what supported contract is violated? | Concrete scenario plus a trace showing why the closest existing path is insufficient or defective |
| Existing capability | Can `redis.Options` configuration, composition, hooks (`DialHook`/`ProcessHook`/`ProcessPipelineHook`), a `PoolHook`, a push `NotificationHandler`, or a caller-owned layer already satisfy the outcome? | Current release code, tests, specs, and an exact supported workflow |
| Compatibility | Is the exported (capitalized) API, module-path/version resolution, RESP protocol, or durable state changed? | Latest release comparison (`version.go`, `RELEASE-NOTES.md`, git semver tag) and contract inspection |
| Solution fit | Is the requested mechanism the best design and implementation layer? | Proposed solution compared with the strongest existing path and at least one narrower or more coherent alternative |
| Resource ownership | Can stale, failed, cancelled, or overlapping work mutate or clean up connections/state owned by surviving work? | Interleaving trace, generation/connID or `ConnState` ownership, and survivor assertions |
| Maintenance cost | What permanent complexity and review burden is added? | New branches/`Options` fields, changed exported surface, tests, and remaining work |

## Severity

- **Negligible**: no runtime difference, unreachable/unsupported input, cosmetic inconsistency, or harmless edge case. Usually close, document, or decline code complexity.
- **Low**: real but narrow and recoverable behavior with a simple workaround and no data, security, or compatibility risk. Merge only when the fix is small and strengthens an invariant.
- **Moderate**: supported use fails or produces incorrect behavior for a meaningful subset. Prioritize a bounded fix and regression test.
- **High**: common or important use is broken, released compatibility is seriously affected, sensitive data can leak, or persistent corruption is possible. Require urgent strong validation.
- **Critical**: broadly exploitable security impact, severe data loss, or systemic failure requiring coordinated action. Use only with concrete evidence.

Severity is consequence multiplied by realistic reach and frequency, reduced by recoverability. Do not raise it because prose is alarming or lower it because a diff is small. Assess reach against the README `Supported versions` policy (the last three Redis releases): behavior reachable only on unsupported versions or unsupported deployments lowers severity and disposition.

## Evidence Strength

Before calling a claim confirmed, answer:

- Does the reproduction exercise the same exported/internal path (root package method, `Cmdable` command, `internal/*` code)?
- Does failure occur on the relevant base branch, latest release (git semver tag / `Version()`), or target?
- Does the regression test fail without the patch and pass with it? For a concurrency defect, does it fail under `go test -race` without the patch?
- Are the rebuilt custom-vet binary (`internal/customvet/customvet` is gitignored and must be rebuilt), wrong module directory in the multi-module workspace, `go.mod`/`go.sum` drift, module-proxy access, `GOCACHE`, runtime knobs (`REDIS_VERSION`, `CLIENT_LIBS_TEST_IMAGE`, `RE_CLUSTER`, `RCE_DOCKER`, `REDIS_PORT`), an unavailable Docker Compose profile, authentication, quota, and service failures excluded?
- Does an equivalent RESP2/RESP3, Client/ClusterClient/Ring/Sentinel/Universal, or pipeline/tx/pubsub path differ?
- Is behavior prohibited by a real contract or merely surprising?
- For latency, timeout, buffering, backpressure, or cleanup, was observable time or state measured against a live Redis (Docker Compose stack) rather than inferred only from a hand-written fake (`mockCmdable`, `redisHookError`) or a `net.Pipe`/`dummyConn` fake socket?
- For shared concurrent state, do tests control goroutine completion order and prove that a stale failure or cleanup cannot affect the surviving operation?

Use `partially confirmed` when the symptom is real but cause/reach/scope is wrong. Use `unproven` when decisive evidence is missing. Use `contradicted` only when evidence directly disproves the claim.

## Unmet Need and Alternative Design Gate

Issue reports often combine a desired outcome with a proposed API or implementation. Treat the proposed mechanism as a hypothesis. Confirm the unmet outcome or violated supported contract before evaluating how well the patch implements that mechanism.

### Linked-evidence scope

Evidence from a linked issue applies only when the issue and PR share the same client variant, protocol (RESP2/RESP3), trigger, supported `redis.Options` configuration, and user outcome. A broad title, ordinary reference, `Related to` statement, or conceptual similarity is not enough. If an earlier change already resolved the concrete reported scenario, an adjacent extension starts with no inherited evidence of need.

### Need evidence status

Assign one status before deep implementation review:

- **Demonstrated**: The exact scope has a concrete supported scenario, real-path reproduction, released compatibility requirement, violated supported contract, repeated demand, or broad invariant with meaningful consequence.
- **Plausible but unproven**: The code path is possible, but realistic reach, frequency, consequence, server/protocol behavior, or demand is missing.
- **Already covered**: A reasonable supported workflow already satisfies the outcome.
- **Unsupported**: The outcome is outside the client's exported contract or belongs at a Redis-server, adapter (`extra/*`), or caller-owned layer.

Only `Demonstrated` need can support a merge-worthy code recommendation. `Plausible but unproven` maps to `Needs evidence` or `Not worth completing`, even when the patch is technically correct and its remaining fixes are bounded. `Already covered` and `Unsupported` normally map to closure or a simpler non-core alternative.

Before accepting an issue or recommending a PR, record:

| Question | Required evidence |
| --- | --- |
| What outcome is needed? | A concrete supported scenario stated without the proposed API or fix |
| What exists today? | The closest current-release API, `redis.Options` field, composition, hook/`PoolHook`/push handler, or caller-owned solution |
| Why is it insufficient? | An exact behavioral, compatibility, lifecycle, or operational constraint, not preference alone |
| What are the alternatives? | The proposed patch, the strongest existing path, and at least one no-code, narrower, or better-layer design |
| Why add a contract? | Practical benefit sufficient to justify exported surface, runtime branches, cross-path tests, documentation, and long-term maintenance |

Classify the result:

- **Capability gap**: a supported, realistic outcome cannot be achieved with current functionality. Code may be warranted.
- **Ergonomics or discoverability gap**: the outcome is already possible, but the supported route is confusing or unnecessarily difficult. Prefer documentation, validation, or a narrowly justified convenience improvement.
- **Unsupported use case**: the desired outcome lies outside the client's exported contract or belongs at the Redis server, an `extra/*` adapter, application, or other caller-owned layer. Do not expand the core API merely to make it possible.
- **No demonstrated gap**: no concrete scenario proves that existing functionality is insufficient. Request evidence or close rather than designing from the proposed mechanism.
- **Defect in supported behavior**: the existing path violates a documented or established correctness, security, compatibility, or lifecycle contract. A workaround may affect priority or solution shape, but does not erase the defect.

Passing tests for a new implementation establish feasibility and correctness, not need. A hand-written fake (`mockCmdable`, `redisHookError`, `mockStreamingProvider`), a `net.Pipe`/`dummyConn` fake socket, an `Options.Dialer` override returning a fake `net.Conn`, or an inline synthetic RESP frame fed to `proto.Reader` proves code-path reachability and parser/plumbing correctness only — it does not establish realistic server behavior, user reach, frequency, consequence, or demand. When real server behavior is the deciding evidence, demonstrate it — with the user's probe approval, per the two-stage evidence flow — against the live Docker Redis stack (`make docker.start` then `make test.ci`, or `make test`) or the e2e fault-injector stack (`make test.e2e`), ideally version-gated with `SkipBeforeRedisVersion`/`SkipAfterRedisVersion`; need itself can equally be demonstrated without executing anything — a concrete supported scenario, a released compatibility requirement, a violated supported contract, or repeated demand. API symmetry and parity with an adjacent client variant are design arguments, not need evidence (see the released-command-surface policy exception below). A technically coherent patch can still be `Not worth completing` when the motivating scenario is hypothetical, already supported, or better solved elsewhere.

Policy exception — released command surface: a PR adding a command, subcommand, or command option that exists in a released, supported Redis or module version has `Demonstrated` need by repository policy; tracking the released command surface is standard go-redis work (see the `add-command` skill). Review of such PRs pivots to implementation fidelity — spec-correct arguments and key positions, a `Cmder` with `SetVal` (the custom-vet `setval` gate), RESP2/RESP3 reply parsing, `SkipBeforeRedisVersion` gating matching the introducing server version, and live-stack tests — rather than demanding a per-user scenario. Convenience re-spellings of functionality the typed API already exposes remain subject to the need gate; the generic `Do(...)` escape hatch does not count as existing exposure for this purpose.

Use the counterfactual maintainer test: if the PR did not already exist, would maintainers choose to file and implement the same work from the available evidence? Contributor effort lowers implementation cost but does not create product need or remove permanent maintenance cost.

When the need is not `Demonstrated`, inspect implementation only far enough to estimate contract, risk, and maintenance cost. Do not convert patch defects, missing tests, or documentation gaps into a request-changes disposition; those become merge blockers only after the need gate passes.

## Issue Disposition

Choose one:

- **Prioritize**: confirmed moderate-or-higher impact or important invariant with no safe workaround.
- **Accept, low priority**: confirmed low impact, existing supported functionality is insufficient or defective for the demonstrated scenario, and a proportionate fix is plausible.
- **Narrow scope**: valid core, overstated paths or expected behavior.
- **Needs evidence**: plausible but missing a supported reproduction, contract basis, or concrete scenario showing why existing functionality is insufficient.
- **Close**: duplicate, unsupported, unreachable, contradicted, no-op, already addressed by a reasonable supported path, or not worth permanent complexity.

Ask only for evidence that could change the disposition.

## PR Quality and Value

Assess independently:

1. **Need**: same-scope evidence demonstrates a concrete unmet user outcome or defect in supported behavior, and the closest supported capability cannot reasonably satisfy the scenario as-is. Do not inherit evidence from an adjacent client variant or already-fixed scenario. Released-command-surface PRs satisfy this item by policy (see the exception in the need gate).
2. **Correctness**: the fix covers the claim and meaningful boundaries.
3. **Placement**: the invariant is enforced once at the owning layer (root package, `internal/pool`, `internal/proto`, `internal/routing`, push `Registry`) instead of duplicating existing functionality, patching locally, or moving caller- or server-owned policy into the core client.
4. **Consistency**: equivalent RESP2/RESP3, Client/ClusterClient/Ring/Sentinel/Universal, pipeline/transaction, and pub/sub paths stay aligned.
5. **Tests**: a regression test fails on base, passes on head, and asserts the non-happy-path value/state. When shared state crosses a goroutine/channel/`context` boundary, tests control relevant completion orders (add `-race`) and assert the surviving operation's behavior and final connection/pool state.
6. **Compatibility**: exported identifiers, module-path/version resolution (`/v9` suffix), RESP2/RESP3 reply shapes, and typed-error behavior (the `Is*` helpers unwrapping via `errors.As`) are preserved or intentionally migrated (`// Deprecated:` alias).
7. **Proportionality**: exported surface and complexity match impact.
8. **Completion cost**: remaining code, tests, docs, design, and conflict work is bounded enough to justify attention.

A PR can be correct but not merge-worthy because the need is negligible, the outcome is already supported through a reasonable existing mechanism, the real path is unchanged, equivalent paths remain inconsistent, the abstraction costs more than the benefit, or a simpler design exists at another layer.

Do not use implementation correctness, bounded remaining work, CI status, or contributor effort to upgrade a need that is only `Plausible but unproven`. Merge-worthiness is gated by demonstrated need, not by how close the patch is to completion.

Keep issue severity separate from `Patch risk`. A patch-induced regression, compatibility break, hook/`PoolHook`/connection leak, leaked `freeTurn`, or maintenance hazard does not make the underlying issue more severe.

## Documentation Threshold

Make docs merge-blocking only when:

- Existing docs (README, `.claude/specs/*.md`, godoc comments) become materially false, unsafe, or misleading.
- Safe/correct use depends on a non-obvious constraint, migration, compatibility boundary, or operational warning.
- Repository policy, accepted scope, or an explicit maintainer decision requires docs in the same PR.
- The feature is practically unusable or undiscoverable without an exported entrypoint and godoc/API discovery is insufficient.

Keep optional discoverability/completeness non-blocking. Do not downgrade a code recommendation solely for optional docs or include optional docs in a required-action paragraph.

## Lifecycle and Failure Paths

Apply this section when a change adds validation, fail-fast behavior, cleanup, retry, interruption, background work (handoff worker, re-auth goroutine), pub/sub, or concurrency.

- Identify the earliest point where all dynamic inputs required for a correct decision exist.
- List side effects before and after that point: hooks, goroutines, channels, `context` cancellations, pub/sub subscriptions, sockets (`net.Conn`), peer connections, pool turns (`waitTurn`/`freeTurn`), files, locks, caches, `ConnState`, persistence, and telemetry.
- Exercise failure during construction, dialing, initialization (`initConn`/re-auth), execution, persistence, and teardown where those phases exist.
- Confirm normal teardown is actually entered. If construction/dial fails, verify explicit cleanup (`Remove` vs `RemoveWithoutTurn`, `Close` of the `net.Conn`).
- Prefer validation after dynamic `redis.Options` configuration is resolved but before avoidable side effects begin.
- Require a regression test for any hook, goroutine, channel, pub/sub subscription, connection, pool turn, file, or `ConnState` that can remain after failure.

## Concurrency and Cleanup Ownership

Apply this section before a positive assessment whenever lifecycle work crosses a goroutine boundary, channel send/receive, `select`, `AwaitAndTransition`, hook/`PoolHook`/push-handler callback, deferred completion (`wantConn` result channel), retry, reconnect, `context` cancellation, or shared connection boundary. Sequential correctness is insufficient because the patch can improve isolated cleanup while introducing cross-goroutine teardown. The single most load-bearing primitive is the `ConnState` machine (`internal/pool/conn_state.go`): `StateUnusable` is the ownership token that lets handoff, re-auth, and normal traffic coexist — trace every `StateUnusable` transition as "who owns this connection right now."

Use a two-operation interleaving matrix during desk review:

| Ordering | Required question |
| --- | --- |
| `A pending -> B starts -> A fails -> B succeeds` | Can A's cleanup (e.g. `ClearHandoffState`, `oldConn.Close()`, `freeTurn`) remove or revert anything B needs? |
| `A pending -> B starts -> B fails -> A succeeds` | Can B's cleanup leave A holding a connection that is successfully returned but wedged (e.g. left `StateUnusable`, or `ShouldHandoff=true`)? |
| `A succeeds -> B starts -> stale A completion` | Can a stale goroutine (worker finishing after `ClearHandoffState`) overwrite B's newer state via a plain `Store` instead of `CompareAndSwap`, resurrecting a stale generation? |
| setup -> Close/cancel -> late completion | Can late work resurrect hooks, `ConnState`, workers, pub/sub subscriptions, or connections after `Manager.Close()` closed the shutdown channel or `context` was cancelled? |

For each ordering:

- Identify the connection owner before and after every goroutine/channel/`AwaitAndTransition` boundary.
- Distinguish per-attempt resources (a single dial attempt's `context`, a `wantConn`) from shared transport, `StickyConnPool`/`SingleConnPool` session, cache, or hook state.
- Require cleanup to carry an ownership token, generation, connID identity check, CAS transition (`CompareAndSwap`, not a plain `Store`), or another invariant that prevents cross-goroutine disposal. The canonical example: `MarkQueuedForHandoff` restores prior state with `CompareAndSwap` precisely so a worker that already ran `ClearHandoffState` is not clobbered and the conn is not wedged as permanently `ShouldHandoff` (`internal/pool/conn.go`).
- Compare base and head on the survivor invariant. Fewer duplicate handoffs do not justify losing the only active connection, leaking a `freeTurn` (permanent capacity loss), or leaving a `wantConn` in the queue with no deliverer.
- Require a controlled interleaving test (`-race`, ordered goroutines) when the ordering is reachable; e2e races between notification arrival and request lifecycle are only caught by `maintnotifications/e2e/` (`make test.e2e`). The test must assert both the failing operation and the surviving operation's observable behavior and final `ConnState`/pool accounting after all goroutines settle.

An unscoped `defer`, `recover()`, `select` cleanup branch, `context`-cancellation handler, or rollback that mutates shared state after a suspension point is merge-blocking when another goroutine can still own or use that connection/state. Flag any new `sync.Mutex`/`sync.RWMutex` on the `Get`/`Put` hot path, and any plain `Store` where a `CompareAndSwap` is required to protect a survivor.

## Better-Alternative Prompts

Start with the strongest existing supported path, then test at least one additional alternative against the proposed patch. Do not complete a positive review without this comparison. For released-command-surface PRs, generic `Do(...)` composition or caller-owned code does not defeat the policy exception.

- Can the requested outcome already be achieved through `redis.Options` configuration, composition, hooks (`DialHook`/`ProcessHook`/`ProcessPipelineHook`), a `PoolHook`, a push `NotificationHandler`, a custom `Options.Dialer`, or caller-owned code?
- If the existing route is awkward, is the problem discoverability or ergonomics rather than missing capability?
- What happens with no code change?
- Can input validation or an existing helper enforce the invariant earlier?
- Can the fix be limited to the supported failing path?
- Would a clearer error (via `cmd.SetErr(wrappedErr)`, preserving `errors.As`) or documentation prevent misuse without runtime complexity?
- Can a failing test reveal a smaller correct change?
- Is a new exported `Options` field compensating for an internal ownership problem in `internal/pool`?
- Can the same result be achieved in the RESP reader/writer (`internal/proto`), the router (`osscluster_router.go`/`internal/routing`), or the `ConnState` owner instead of every caller?
- Is the proposed core behavior actually server- or application-specific policy that belongs at the Redis server, an `extra/*` adapter, or another layer?

## Competing Pull Requests

Require an explicit issue link, same reproduction, same violated invariant, or materially overlapping runtime path before grouping candidates.

| Criterion | Question |
| --- | --- |
| Need | Does a concrete user outcome remain unmet, or supported behavior remain defective, after tracing existing functionality? |
| Existing capability | Could every candidate be avoided by `redis.Options` configuration, composition, a hook/`PoolHook`/push handler, or a better caller- or `extra/*`-owned solution? |
| Coverage | Whole confirmed issue, useful subset, or adjacent problem? |
| Correctness | Real path and meaningful boundaries? |
| Placement | Owning shared layer (root package, `internal/*`)? |
| Tests | Base failure reproduced (with `-race` for concurrency) and approaches distinguished? |
| Compatibility | Exported API, module/version resolution, RESP2/RESP3, durable state, client variants? |
| Complexity | Permanent branches, abstractions, `Options` fields, coupling? |
| Readiness | Mergeable now or bounded focused work? |
| Reuse | Exact tests or ideas worth transferring? |

Choose one portfolio action:

- **Prefer one PR**
- **Prefer one after focused changes**
- **Combine selectively** into a named destination PR
- **Replace all** with a simpler/coherent implementation
- **Merge none**

Do not issue independent approvals for overlapping candidates. State the action for every active PR.

## Maintainer Comments

Write drafts in English. Produce one when recommending closure, more evidence, focused changes, superseding, or choosing among competing PRs.

Keep it polite, direct, complete, and usually 60-160 words in one to three short paragraphs:

1. Acknowledge the contribution/report.
2. State the decision with decisive technical evidence.
3. Give the exact next action or reconsideration condition.

Do not include internal severity labels, speculate about authorship/intent, repeat the full review, or soften the requested action until it is unclear. Any suggested commit/PR text must follow Conventional Commits (`<type>(<scope>): <imperative summary>`, subject <= 50 chars, the repo scope vocabulary) and add NO AI-attribution trailer (no `Co-Authored-By`, no "Generated with").

### Close

```text
Thanks for taking the time to investigate this. I traced the reported case through <path or behavior>, and <decisive finding>. In the supported path, <practical result>, so the added complexity is not justified by the demonstrated impact.

I am going to close this <issue/PR>. If you can provide <specific reproduction or evidence that would change the decision>, we can revisit the underlying problem with that narrower scope.
```

### Request Changes

```text
Thanks for the contribution. The underlying issue is valid, and this approach is directionally reasonable. Before we can merge it, please address the following points: <bounded required changes>.

These changes are needed because <contract, lifecycle, compatibility, or test reason>. Once they are covered with a regression test that fails on the base and passes on the updated branch (add `-race` if the fix is concurrency-related), the PR should be ready for another review.
```

Adapt these templates to evidence. Do not use them as filler.

### Existing Capability or Better Alternative

```text
Thanks for the contribution. I traced the underlying use case through <existing API, redis.Options field, hook, or workflow>, which already supports <desired outcome and relevant limits>. The proposed change adds <new exported contract or complexity>, but the issue does not demonstrate a concrete supported case that the existing approach cannot handle.

I am going to close this <issue/PR> for now. If you can provide <specific scenario showing the existing approach is insufficient>, we can revisit the unmet need and choose the narrowest appropriate design from that evidence.
```

## Compact Report Variants

Use `Maintainer decision` for a concluded review. Use `Preliminary assessment` when a desk review is tentatively positive but a decision-relevant runtime concern remains. `Verdict` is intentionally avoided in the report headings because it does not communicate whether the result is provisional or final.

### Runtime Approval Gate

```markdown
## Preliminary assessment

<Tentative issue or PR assessment based on desk review only.>

## Static evidence

- <decisive code-path or test-inspection evidence>
- <what remains uncertain at runtime>

## Proposed runtime probe

- Concern: <the uncertainty that could change the decision>
- Probe: <smallest exact execution path, e.g. `make docker.start` then `go test -run TestGinkgoSuite . -ginkgo.focus="..." -race`, a plain `go test -run TestName ./internal/pool/...`, `make test.ci`, or the `make test.e2e` fault-injector stack>
- Control: <base branch, released tag, or known-good comparison when relevant>
- Scope: <Docker Compose profile required (standalone/cluster/sentinel/ring/cluster-tls/all/e2e), REDIS_VERSION and other knobs, and any live-service, mutation, or cleanup implications; gated on Docker availability>

## Approval request

<Ask whether to run this exact probe. Do not present a final positive recommendation yet. GitHub access stays read-only; no comments, labels, branch changes, pushes, or merges.>
```

### Issue

```markdown
## Maintainer decision

<Real/partial/unproven/contradicted, severity, and disposition.>

## Evidence

- <decisive evidence>
- <scope or uncertainty>

## Existing capability and alternatives

<Closest supported path (redis.Options, hook, PoolHook, push handler, composition), why it is or is not sufficient, and the preferred design alternative.>

## Recommendation

<Prioritize, accept low priority, narrow, request evidence, or close.>

## Maintainer comment draft

<Include for closure or an evidence request.>
```

### Pull Request

```markdown
## Maintainer decision

<Need, practical impact, and merge-worthiness.>

- Need evidence: <Demonstrated / Plausible but unproven / Already covered / Unsupported>
- Code recommendation: <code disposition; omit when the need is not `Demonstrated` and the action is an evidence request>
- Repository readiness: <one allowed status; only when useful for a merge-worthy recommendation>

## Evidence

- <runtime/code-path result>
- <test/compatibility result>

## Existing capability and alternatives

<Closest supported path, why the demonstrated scenario cannot use it or remains defective, and why this patch is preferable to no code change or a narrower design.>

## Issue impact

- Validity: <claim validity>
- Severity: <underlying issue severity>
- Reach: <realistic reach across client variants / protocols / versions>

## Patch risk

<Only meaningful patch-induced risk (regression, compatibility break, hook/PoolHook/connection leak, leaked freeTurn, wedged ConnState, race).>

## PR quality

- Solution fit: <assessment>
- Tests: <assessment (base-fail/head-pass, -race where concurrent, version-gated)>
- Remaining effort: <bounded/unbounded and why>

## Recommendation

<Merge, focused changes, simpler replacement, or close.>

## Maintainer comment draft

<Only when closure, evidence, or changes should be requested.>
```

### Competing Pull Requests

```markdown
## Maintainer decision

<Issue validity, severity, and preferred implementation path.>

## Open PR comparison

| PR   | Approach | Correctness | Tests | Compatibility/complexity | Readiness |
| ---- | -------- | ----------- | ----- | ------------------------ | --------- |
| #... | ...      | ...         | ...   | ...                      | ...       |

## Recommendation

<Select one, request focused changes, combine exact pieces, replace all, or merge none.> <State the action for every other active candidate.>

## Maintainer comment drafts

<One draft for each PR that should be closed, changed, or superseded.>
```
