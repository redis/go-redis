# TS.QUERYLABELS example

This example demonstrates `TSQueryLabels` and `TSQueryLabelValues`, the
client bindings for the two forms of `TS.QUERYLABELS` (Redis 8.10+):

- `TSQueryLabels(ctx, filters)` — the set of label names present on the
  matching time series (`TS.QUERYLABELS LABELS [FILTER ...]`).
- `TSQueryLabelValues(ctx, label, filters)` — the set of values assigned to
  one label across the matching series
  (`TS.QUERYLABELS VALUES label [FILTER ...]`).

Together with the existing `TSQueryIndex` they form the drill-down flow
dashboarding tools such as Grafana use to populate variable dropdowns:
label names → values of the chosen label → matching series keys.

Key points shown by the example:

- Filters use the same expression language as `TSQueryIndex`/`TSMRange` and
  are passed to the server verbatim; passing no filters queries all indexed
  series.
- Replies are unordered, deduplicated sets of strings; the `LABELS` form
  includes the label names used in the filter itself.
- A label carried by no matching series yields an empty reply, not an error.

## Prerequisites

Redis 8.10+ with the TimeSeries module, listening on `localhost:6379`.

## Run

```shell
go run .
```

Expected output (order within the collections may differ):

```
label names for type=sensor: [location type unit]
locations for type=sensor:   [bedroom kitchen]
kitchen sensor series:       [ts:hum:kitchen ts:temp:kitchen]
values of an absent label:   0
label names, all series:     [location type unit]
```
