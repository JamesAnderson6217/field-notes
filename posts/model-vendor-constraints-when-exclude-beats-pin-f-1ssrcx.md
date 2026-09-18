# Model Vendor Constraints — When Exclude Beats Pin for Prepaid Routing

**TL;DR:** For a B2B SaaS account that must not exhaust a prepaid model balance unattended, make exclusion the normal failover constraint and reserve a vendor pin for obligations that truly require one provider. Exclusion preserves more eligible capacity as providers and models change; a pin gives stronger attribution but creates a single-provider availability boundary. Neither rule should debit money. Admission should reserve an internal budget first, routing should select from the remaining eligible providers, and reconciliation should replace the reservation with attributable usage after the call.

That separation matters more than the syntax of `allow` or `deny`. A routing decision can be exactly reproducible and still be financially wrong if a retry is charged twice, if usage arrives without the tenant and request identifiers that justified it, or if an emergency exclusion silently changes which prepaid account pays. Treat the route decision as audited evidence attached to a ledger operation, not as a clever filter attached to an HTTP request.

## Should routing pin one model vendor or exclude one?

A pin defines one acceptable answer. If the named provider becomes ineligible because its prepaid balance crosses the account's reserve threshold, the request must wait or fail; silently escaping the pin would make the constraint dishonest. An exclusion defines a forbidden answer and leaves every other provider eligible, so the candidate set can expand without rewriting old tenant policy. This is an availability advantage, not proof that the resulting bill is attributable.

The useful distinction is therefore semantic. Use a pin when the provider is part of the customer's contract, data-processing boundary, model qualification, or invoice mapping. Use an exclusion when the rule represents a temporary incident response, a tenant prohibition, a key-rotation window, or a provider-specific operational concern. A pin should be uncommon and explicit. Exclusions can compose, but their intersection can still empty the candidate set.

There is no universal winner.

Fail closed.

When no candidate satisfies policy and balance admission, return a typed routing error before sending work. Falling back outside policy might keep a request alive while assigning its spend to the wrong prepaid pool, which is precisely the failure this design is meant to prevent.

| Constraint | Candidate-set behavior | Balance exhaustion behavior | Attribution implication |
|---|---|---|---|
| Pin `A` | Only `A` is eligible | Stop when `A` cannot admit the reservation | Stable provider dimension, narrow availability |
| Exclude `A` | Any configured provider except `A` is eligible | Continue only through independently admitted providers | Provider must be recorded per attempt |
| No constraint | All policy-compatible providers are eligible | Continue while any provider can admit the reservation | Selection policy and chosen provider must both be recorded |

The table also exposes a trap: an exclusion is not a fallback order. Ordering is a separate, versioned policy. Conflating the two makes an innocent priority edit reinterpret every stored deny rule.

## Put balance admission before provider selection

The account platform should own a ledger denominated in the billing unit it promises to customers. Before dispatch, it creates a reservation large enough for the permitted request. The router then evaluates policy against provider pools whose available balance can cover that reservation. Once final attributable usage is known, reconciliation commits the actual debit and releases the remainder; a failed attempt releases or expires its reservation according to a documented state transition.

This is an exactly-once mindset applied to effects, not a claim that networks deliver exactly once. Every logical operation needs an idempotency key, while every physical provider attempt needs its own attempt identifier. Reusing the logical key prevents a client retry from reserving twice. Keeping attempt IDs distinct preserves the uncomfortable truth that two upstream attempts may both have executed and may both appear in later usage records.

The following Go sketch keeps policy evaluation pure. Monetary mutation belongs behind `Admit`, where a transactional ledger can enforce uniqueness; the selector merely returns a decision that can be persisted with the reservation.

```go
package routing

import (
	"errors"
	"sort"
)

var ErrNoEligibleProvider = errors.New("no provider satisfies policy and balance admission")

type Constraint struct {
	Pin     string
	Exclude map[string]bool
}

type Pool struct {
	Provider      string
	AvailableUnit int64
	Priority      int
}

type Decision struct {
	Provider      string
	PolicyVersion string
	ReservedUnit  int64
}

func Select(pools []Pool, c Constraint, reserve int64, policyVersion string) (Decision, error) {
	candidates := append([]Pool(nil), pools...)
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].Priority < candidates[j].Priority
	})

	for _, pool := range candidates {
		if c.Pin != "" && pool.Provider != c.Pin {
			continue
		}
		if c.Exclude[pool.Provider] || pool.AvailableUnit < reserve {
			continue
		}
		return Decision{
			Provider:      pool.Provider,
			PolicyVersion: policyVersion,
			ReservedUnit:  reserve,
		}, nil
	}
	return Decision{}, ErrNoEligibleProvider
}
```

Do not infer a missing balance as zero or infinity. It is unknown, and an unattended continuity system should make that state visible rather than convert it into a financial guess. Likewise, use integers in the ledger's declared unit rather than floating-point arithmetic, and preserve the original usage record beside any normalized amount so reconciliation remains explainable.

## Make attribution survive retries and policy changes

The audit record needs enough information to reproduce why an attempt was allowed: tenant account, logical operation key, attempt ID, reservation ID, selected provider, constraint type and value, policy version, credential reference, timestamps, and reconciliation state. The credential reference is an identifier, never the secret itself. OWASP's secrets-management guidance recommends centralized lifecycle management, least privilege, rotation, expiration, and auditing; those controls belong around provider credentials even when route policy is stored elsewhere.

There is a compliance limit worth stating plainly: a detailed route log is not automatically an acceptable audit trail. Retention, access, deletion, regional handling, and the fields considered sensitive depend on the organization's obligations. Store the minimum evidence needed to reconcile a charge, protect it from casual mutation, and have compliance owners approve the retention boundary. Never log API keys, authorization headers, or raw request bodies merely to make debugging easier.

Retries create the hardest attribution case. Suppose operation `op-731` reserves 40 internal units, attempt `a1` is sent to provider A, and the caller times out before receiving a definitive result. Excluding A and sending attempt `a2` to provider B does not prove that `a1` incurred no usage. Keep the original reservation open until the ambiguity is resolved or its documented accounting deadline passes; if a second reservation is allowed, link both attempts to the same logical operation and reconcile both upstream records. The customer-facing ledger can then apply the contract's duplicate-execution rule without erasing provider-side facts. An operator inspecting the account should see one logical operation, two physical attempts, the policy version evaluated for each attempt, and every upstream usage item that later arrived. If the prepaid pool cannot cover the possible effect of both attempts, the second admission must stop even though retrying might improve availability. That trade-off is deliberate: uninterrupted dispatch cannot take precedence over the balance boundary the system promised to enforce.

This is why the route event and the money event should be joined, not merged. Routing answers “where did this attempt go?” The ledger answers “what financial effect was authorized and ultimately recognized?” One mutable row cannot answer both questions reliably after a retry.

## Test the invariants, not just the happy route

Unit tests should establish candidate-set semantics: a pin never escapes, an excluded provider never returns, and an empty set yields the typed error. Property tests add more value than another table of examples: adding an unrelated provider may expand an exclusion-based candidate set, but must never change a pinned result; adding an exclusion may shrink the set, but must never introduce a candidate.

Concurrency tests belong at the ledger boundary. Launch several admissions against the last available prepaid units and verify that committed reservations never exceed the account balance, that the same idempotency key returns the same logical reservation, and that distinct attempts remain traceable. Then exercise delayed usage, duplicate usage, a timeout after dispatch, credential rotation during an in-flight request, and a policy update between reservation and reconciliation.

Observability should follow the same boundaries. Count admission rejections by reason, candidate-set exhaustion, ambiguous attempts, reservation age, and unreconciled usage; alert on sustained changes rather than individual expected rejections. Keep tenant identifiers controlled, use low-cardinality labels for metrics, and put request-level detail in access-restricted audit storage. The operational question is not merely “did routing succeed?” It is “can every admitted effect be explained and reconciled?”

## Compare constraints on the axis that matters

For prepaid continuity, exclusion ages better when provider membership changes often and any eligible provider satisfies the workload. A pin ages better when attribution must remain attached to a predetermined provider pool, even at the cost of stopping sooner. The decision rule is compact: **exclude for negative eligibility; pin for positive obligation**.

The limitation of exclusion is its broader attribution surface: every attempt must carry the selected provider and credential-account reference, and downstream invoicing must accept that a single tenant can consume several provider pools. It is unsuitable when a contract permits only one provider, when qualification covers one model-provider pairing, or when reconciliation cannot ingest per-attempt provider identity. In those cases, choose a pin and accept its narrower availability boundary. Conversely, a pin is a poor fit for a workload whose overriding requirement is unattended continuity across interchangeable providers; use exclusion plus independently admitted pools there.

Do not hide a preference inside either primitive. Latency, model capability, regional eligibility, credential health, and balance admission are filters or ranking inputs with independent versions. Keeping them separate lets an operator explain a decision without pretending that `exclude A` meant `prefer B`, or that `pin A` authorized a different credential account after rotation.

Cost still matters, but it is not the routing thesis. Normalize usage only after retaining source evidence, and never choose a route from a stale price assumption when the account promise is “do not run out unattended.” The stronger control is a conservative reservation plus reconciliation, because it caps admitted exposure even when final usage is delayed.

The constraint cannot repair weak accounting.

## Roll out without rewriting financial history

Start in shadow mode: evaluate both constraint forms, persist the proposed decision, and leave dispatch unchanged. Compare candidate-set emptiness and attribution completeness, not hypothetical savings. Next, enforce exclusions for a small tenant cohort while pins remain explicit opt-ins; keep a kill switch that stops new admissions without altering already recorded decisions.

During migration, version every policy and preserve the evaluated version on each attempt. Backfills may add missing references, but they must not recalculate an old route under today's candidate set. Once reconciliation lag, ambiguous-attempt counts, and ledger invariants remain within the organization's approved operating limits, expand enforcement and retire the shadow evaluator.

The lasting architecture is modest: immutable evidence, idempotent admission, explicit candidate semantics, and reconciliation that accepts uncertainty instead of deleting it. Under those constraints, exclusions provide flexible continuity, while pins remain the deliberate exception for obligations that flexibility cannot satisfy.

## Sources

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
