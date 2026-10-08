# Workflow dispatch query plans — 2026-10-08

Captured by `workflow_dispatch_live_query_plans` on the second ephemeral Aurora
DSQL cluster. These are `EXPLAIN` plans for the repository query builders, with
fixture UUIDs and digest values replaced by placeholders. Costs, row estimates,
predicates and index names are unchanged. The query-plan test does not use
`ANALYZE`; these plans do not measure rows read or elapsed query time.

See [implementation evidence](implementation-evidence.md) for lifecycle times,
validation outcomes and the deferred Task 15 work.

```text
Seeded rows: 8192; table rows after seeding: 9517.
128 queue families; Live and Exact each contain 32 rows per seeded family.
32 execution homes contain 256 seeded rows each.
ASYNC indexes were awaited by the migration runner before seeding.

Queue mode 0, continuation=false, range selectivity=0.003362:
Limit  (cost=201.53..209.64 rows=6 width=171)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=201.53..209.64 rows=6 width=171)
        Index Cond: ((queue_namespace = '<fixture-uuid>'::uuid) AND (queue_key = '<fixture-digest>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest>'::text) AND (build_key = '<fixture-digest>'::text))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=201.53..209.64 rows=6 width=171 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=201.53..209.64 rows=6 width=171 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid>'::uuid) AND (queue_key = '<fixture-digest>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest>'::text) AND (build_key = '<fixture-digest>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=201.53..209.64 rows=6 width=171 loops=1)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=201.53..209.64 rows=6 width=171 loops=1)

Queue mode 0, continuation=true, range selectivity=0.003362:
Limit  (cost=202.53..210.80 rows=1 width=171)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=202.53..210.80 rows=1 width=171)
        Index Cond: ((queue_namespace = '<fixture-uuid>'::uuid) AND (queue_key = '<fixture-digest>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest>'::text) AND (build_key = '<fixture-digest>'::text))
        Filter: (ROW(priority_key, scheduled_at, run_key) > ROW('3'::smallint, '2023-11-14 22:13:20+00'::timestamp with time zone, '<fixture-uuid>'::uuid))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=202.53..210.80 rows=10 width=171 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=202.53..210.80 rows=10 width=171 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid>'::uuid) AND (queue_key = '<fixture-digest>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest>'::text) AND (build_key = '<fixture-digest>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=202.53..210.80 rows=10 width=171 loops=1)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=202.53..210.80 rows=10 width=171 loops=1)

Queue mode 1, continuation=false, range selectivity=0.003362:
Limit  (cost=201.03..209.11 rows=4 width=171)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=201.03..209.11 rows=4 width=171)
        Index Cond: ((queue_namespace = '<fixture-uuid>'::uuid) AND (queue_key = '<fixture-digest>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest>'::text) AND (build_key = '<fixture-digest>'::text))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=201.03..209.11 rows=4 width=171 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=201.03..209.11 rows=4 width=171 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid>'::uuid) AND (queue_key = '<fixture-digest>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest>'::text) AND (build_key = '<fixture-digest>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=201.03..209.11 rows=4 width=171 loops=1)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=201.03..209.11 rows=4 width=171 loops=1)

Queue mode 1, continuation=true, range selectivity=0.003362:
Limit  (cost=201.03..209.14 rows=1 width=171)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=201.03..209.14 rows=1 width=171)
        Index Cond: ((queue_namespace = '<fixture-uuid>'::uuid) AND (queue_key = '<fixture-digest>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest>'::text) AND (build_key = '<fixture-digest>'::text))
        Filter: (ROW(priority_key, scheduled_at, run_key) > ROW('3'::smallint, '2023-11-14 22:13:20+00'::timestamp with time zone, '<fixture-uuid>'::uuid))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=201.03..209.14 rows=4 width=171 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=201.03..209.14 rows=4 width=171 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid>'::uuid) AND (queue_key = '<fixture-digest>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest>'::text) AND (build_key = '<fixture-digest>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=201.03..209.14 rows=4 width=171 loops=1)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=201.03..209.14 rows=4 width=171 loops=1)

Home continuation=false, matching rows=448, selectivity=0.047074:
Limit  (cost=156.03..160.01 rows=64 width=16)
  ->  Index Only Scan using idx_workflow_dispatch_home on workflow_dispatch  (cost=156.03..183.84 rows=448 width=16)
        Index Cond: (shard_id = '<fixture-uuid>'::uuid)
        -> Storage Scan on idx_workflow_dispatch_home  (cost=156.03..183.84 rows=448 width=16 loops=1)
            Projections: run_key
            -> B-Tree Scan on idx_workflow_dispatch_home  (cost=156.03..183.84 rows=448 width=16 loops=1)
                Index Cond: (shard_id = '<fixture-uuid>'::uuid)

Home continuation=true, matching rows=448, selectivity=0.047074:
Limit  (cost=125.53..130.00 rows=64 width=16)
  ->  Index Only Scan using idx_workflow_dispatch_home on workflow_dispatch  (cost=125.53..139.78 rows=204 width=16)
        Index Cond: ((shard_id = '<fixture-uuid>'::uuid) AND (run_key > '<fixture-uuid>'::uuid))
        -> Storage Scan on idx_workflow_dispatch_home  (cost=125.53..139.78 rows=204 width=16 loops=1)
            Projections: run_key
            -> B-Tree Scan on idx_workflow_dispatch_home  (cost=125.53..139.78 rows=204 width=16 loops=1)
                Index Cond: ((shard_id = '<fixture-uuid>'::uuid) AND (run_key > '<fixture-uuid>'::uuid))
```
