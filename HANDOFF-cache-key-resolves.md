# Graphiti: `stale?` resolves the whole graph two to three times

## The problem

A controller doing the standard graphiti thing:

```ruby
resource = controller_resource_record
authorize resource.data                      # resolves the graph
respond_with resource if stale?(resource)    # resolves it again, twice
```

Rails' `stale?` derives the etag from `cache_key_with_version` and last-modified from
`updated_at`. Both land in `Graphiti::Scope#sideload_resource_proxies`, which resolves
data that the request has already resolved.

Measured with a probe counting `Adapter#resolve` calls (PORO fixtures, 10 parents):

| include depth | render alone | with `stale?` | before fix | after fix |
| --- | --- | --- | --- | --- |
| 1 sideload | 2 | 8 → 6 | 4.0× | 3.0× |
| 2 sideloads | 9 | 24 → 20 | 2.7× | 2.2× |
| 3 sideloads | 20 | 48 → 42 | 2.4× | 2.1× |

Confirmed on real traffic: removing `stale?` from the app's `DefaultActions` cut find
p50 by 11% and roughly halved index requests (80–260 ms → 47–75 ms) on a 10 MB
JSON:API response with eleven sideloads.

## What is already done

Branch `perf/reuse-resolved-for-cache-key`, commit `6336c75e`, off `beta`.

`Scope#resolve_primary_data` now stores its result in `@resolved_records`, and
`sideload_resource_proxies` uses it instead of calling `before_resolve` + `resolve`
again. That removes the root scope's re-resolution only.

`1641 examples, 0 failures`; rails-7-1 and rails-8-0 `349 examples, 0 failures`; standardrb clean.

## What remains

`sideload_resource_proxies` still builds a **fresh proxy per sideload**
(`lib/graphiti/scope.rb`, the `sideload.build_resource_proxy(results, q, parent_resource)`
line). Those proxies have never resolved, so each one resolves its own data, and so do
theirs, recursively. That is the remaining 2.1×.

The data is already in memory: after resolution the sideload has assigned
`parent.public_send(sideload.association_name)`. So the child proxy's scope could be
seeded with those records rather than re-querying, and the same recursion would collapse
the whole tree to zero extra resolutions.

Sketch:

```ruby
children = results.flat_map { |r| r.public_send(sideload.association_name) }.compact
proxy = sideload.build_resource_proxy(results, q, parent_resource)
proxy.scope.seed_resolved(children)   # new public method setting @resolved_records
```

## The risk, which is why this is not done yet

**Not every sideload assigns its association.** A custom `assign` block may do work
without writing anything back. Real example from the app that motivated this:

```ruby
has_many :intersecting, resource: AiringResource, foreign_key: :id do
  assign do |airings, _intersections|
    airings.each(&:intersecting)   # loads an AR association, assigns nothing
  end
end
```

Seeding from an unassigned association either lazy-loads (ActiveRecord) or yields
nothing (PORO). Either way the cache key gets computed from incomplete data, and the
failure mode is a **stale etag**: the response changes but its key does not, so clients
keep a stale copy. Silent, and not caught by any existing spec.

So the seeding needs:

- a per-adapter loaded-ness check (ActiveRecord: `record.association(name).loaded?`),
- a conservative fallback to re-resolving whenever loadedness cannot be established,
- specs asserting the etag still **changes** when underlying data changes, at every
  include depth, and for a sideload with a custom `assign` that writes nothing back.

That last spec is the important one. It is the regression this change could introduce.

## Also worth considering

`Scope#updated_at` issues `@object.maximum(:updated_at)` per sideload proxy on top of
the resolutions. Cheap aggregates individually, but one per relation per request.

And `AiringResource` in the motivating app documents that it must not be cached, yet the
endpoint computes a full etag and last-modified for it on every request. Where a response
is genuinely uncacheable, not calling `stale?` is strictly better than making it cheap.

## Reproducing the measurement

```ruby
# spec/stale_probe_spec.rb, delete after use
def resolves
  $resolves = 0
  PORO::Adapter.prepend(Module.new do
    def resolve(scope) = ($resolves += 1) && super
  end)
  yield
  $resolves
end

params = {page: {size: 10}, include: "positions.department"}
render_only = resolves { PORO::EmployeeResource.all(params).to_jsonapi }
like_show = resolves do
  p = PORO::EmployeeResource.all(params)
  p.data; p.cache_key_with_version; p.updated_at; p.to_jsonapi
end
```

Allocation baselines live in `spec/performance/baselines.yml`; regenerate with
`rake performance:baseline`. They will not move for this change — it is query count and
wall time, not allocations.
