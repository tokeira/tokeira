# Workflow dispatch reconciliation: continuation plans

Recorded on real Aurora DSQL on 2026-10-08, through SQLx and the repository's
`workflow_dispatch_live_query_plans` contract. Each after-plan uses the exact
`queue_page_query` builder used by the storage read path. The before-plan uses
the prior tuple-comparison query as a negative control. Fixture UUIDs and digests
are replaced consistently below; no resource identities are included.

The fixture seeds 16,384 rows; the serial acquisition and ordering contracts leave
29 other rows, making 16,413 total. Each target range contains 4,096 rows (24.9558%
of the table), all in priority band 3, grouped into four equal-time groups of
1,024 rows. Offsets 768 and 3,840 lie within an equal-time group; offset 2,048
starts immediately after a complete group. Mode 0 is Live; mode 1 is Exact.
Pages contain at most 64 candidates. Complete traversal returns all 4,096 ordered
positions in each range without omissions or duplicates.

The old continuation's ordering tuple remains a residual `Filter`. The new
continuation has three disjoint index-only seeks, each with its full scalar
ordering bounds in `Index Cond`: equal priority/time plus greater run key;
equal priority plus greater time; and greater priority. A materialized page
bounds the batched primary-key equality lookups to at most 64 payloads. The
consumed prefix is outside every seek interval. First pages and home pages use
their corresponding indexes. These plans establish the observed planner behavior
for this fixture; the executable assertions detect regressions on later runs.

No migration or additional concurrent query permit is needed. The single SQL
statement retains one transaction snapshot and returns one page per discovery
slice. Each seek child is independently limited before the outer page limit.

```text
Seeded rows: 16384; table rows after seeding: 16413.
Live and Exact each contain 4096 rows in queue 0, all priority 3, with 1024 equal timestamps per group. Another 8192 rows span 128 unrelated queue families.
64 execution homes contain 256 seeded rows each.
ASYNC indexes were awaited by the migration runner before seeding.

Queue mode 0, offset=0, before=true, range selectivity=0.249558:
Limit  (cost=417.28..423.78 rows=64 width=172)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=417.28..505.53 rows=869 width=172)
        Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=417.28..505.53 rows=869 width=172 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=417.28..505.53 rows=869 width=172 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=417.28..505.53 rows=869 width=172 loops=1)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=417.28..505.53 rows=869 width=172 loops=1)

Queue mode 0, offset=0, before=false, range selectivity=0.249558:
Sort  (cost=407.81..407.97 rows=64 width=402)
  Sort Key: candidates.priority_key, candidates.scheduled_at, intent.run_key
  CTE candidates
    ->  Limit  (cost=208.66..212.49 rows=64 width=26)
          ->  Limit  (cost=208.66..212.49 rows=64 width=26)
                ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=208.66..260.73 rows=869 width=26)
                      Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
                      -> Storage Scan on idx_workflow_dispatch_queue  (cost=208.66..260.73 rows=869 width=26 loops=1)
                          Projections: priority_key, scheduled_at, run_key
                          -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=208.66..260.73 rows=869 width=26 loops=1)
                              Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
  ->  Nested Loop (Batched Join)  (cost=171.03..193.40 rows=64 width=402)
        Recheck Cond: (run_key = candidates.run_key)
        ->  CTE Scan on candidates  (cost=0.00..1.28 rows=64 width=26)
        ->  Index Only Scan using workflow_dispatch_pkey on workflow_dispatch intent  (cost=171.03..177.14 rows=64 width=376)
              Index Cond: (run_key = candidates.run_key)
              -> Storage Scan on workflow_dispatch_pkey  (cost=171.03..177.14 rows=64 width=376 loops=1)
                  Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, queue_key, deployment_key, build_key, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
                  -> B-Tree Scan on workflow_dispatch_pkey  (cost=171.03..177.14 rows=64 width=376 loops=1)
                      Index Cond: (run_key = candidates.run_key)

Queue mode 0, offset=768, before=true, range selectivity=0.249558:
Limit  (cost=417.28..512.05 rows=1 width=172)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=417.28..512.05 rows=1 width=172)
        Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
        Filter: (ROW(priority_key, scheduled_at, run_key) > ROW('3'::smallint, '2023-11-14 22:13:20+00'::timestamp with time zone, '<fixture-uuid-2>'::uuid))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=417.28..512.05 rows=869 width=172 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=417.28..512.05 rows=869 width=172 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=417.28..512.05 rows=869 width=172 loops=1)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=417.28..512.05 rows=869 width=172 loops=1)

Queue mode 0, offset=768, before=false, range selectivity=0.249558:
Sort  (cost=596.21..596.37 rows=64 width=402)
  Sort Key: candidates.priority_key, candidates.scheduled_at, intent.run_key
  CTE candidates
    ->  Limit  (cost=397.91..400.89 rows=64 width=26)
          ->  Merge Append  (cost=397.91..403.92 rows=129 width=26)
                Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                ->  Sort  (cost=115.75..115.91 rows=64 width=26)
                      Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                      ->  Limit  (cost=108.28..113.83 rows=64 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=108.28..114.00 rows=66 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '2023-11-14 22:13:20+00'::timestamp with time zone) AND (run_key > '<fixture-uuid-2>'::uuid))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=108.28..114.00 rows=66 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=108.28..114.00 rows=66 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '2023-11-14 22:13:20+00'::timestamp with time zone) AND (run_key > '<fixture-uuid-2>'::uuid))
                ->  Sort  (cost=181.98..182.14 rows=64 width=26)
                      Sort Key: workflow_dispatch_1.priority_key, workflow_dispatch_1.scheduled_at, workflow_dispatch_1.run_key
                      ->  Limit  (cost=176.03..180.06 rows=64 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_1  (cost=176.03..214.32 rows=608 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '2023-11-14 22:13:20+00'::timestamp with time zone))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=176.03..214.32 rows=608 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=176.03..214.32 rows=608 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '2023-11-14 22:13:20+00'::timestamp with time zone))
                ->  Limit  (cost=100.16..104.18 rows=1 width=26)
                      ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_2  (cost=100.16..104.18 rows=1 width=26)
                            Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key > '3'::smallint))
                            -> Storage Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                Projections: priority_key, scheduled_at, run_key
                                -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                    Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key > '3'::smallint))
  ->  Nested Loop (Batched Join)  (cost=171.03..193.40 rows=64 width=402)
        Recheck Cond: (run_key = candidates.run_key)
        ->  CTE Scan on candidates  (cost=0.00..1.28 rows=64 width=26)
        ->  Index Only Scan using workflow_dispatch_pkey on workflow_dispatch intent  (cost=171.03..177.14 rows=64 width=376)
              Index Cond: (run_key = candidates.run_key)
              -> Storage Scan on workflow_dispatch_pkey  (cost=171.03..177.14 rows=64 width=376 loops=1)
                  Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, queue_key, deployment_key, build_key, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
                  -> B-Tree Scan on workflow_dispatch_pkey  (cost=171.03..177.14 rows=64 width=376 loops=1)
                      Index Cond: (run_key = candidates.run_key)

Queue mode 0, offset=2048, before=true, range selectivity=0.249558:
Limit  (cost=417.28..512.05 rows=1 width=172)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=417.28..512.05 rows=1 width=172)
        Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
        Filter: (ROW(priority_key, scheduled_at, run_key) > ROW('3'::smallint, '2023-11-14 22:13:21+00'::timestamp with time zone, '<fixture-uuid-3>'::uuid))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=417.28..512.05 rows=869 width=172 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=417.28..512.05 rows=869 width=172 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=417.28..512.05 rows=869 width=172 loops=1)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=417.28..512.05 rows=869 width=172 loops=1)

Queue mode 0, offset=2048, before=false, range selectivity=0.249558:
Sort  (cost=554.95..555.11 rows=64 width=402)
  Sort Key: candidates.priority_key, candidates.scheduled_at, intent.run_key
  CTE candidates
    ->  Limit  (cost=354.74..359.63 rows=64 width=26)
          ->  Merge Append  (cost=354.74..359.78 rows=66 width=26)
                Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                ->  Sort  (cost=104.19..104.20 rows=1 width=26)
                      Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                      ->  Limit  (cost=100.16..104.18 rows=1 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=100.16..104.18 rows=1 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '2023-11-14 22:13:21+00'::timestamp with time zone) AND (run_key > '<fixture-uuid-3>'::uuid))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '2023-11-14 22:13:21+00'::timestamp with time zone) AND (run_key > '<fixture-uuid-3>'::uuid))
                ->  Sort  (cost=150.37..150.53 rows=64 width=26)
                      Sort Key: workflow_dispatch_1.priority_key, workflow_dispatch_1.scheduled_at, workflow_dispatch_1.run_key
                      ->  Limit  (cost=144.03..148.45 rows=64 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_1  (cost=144.03..168.30 rows=352 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '2023-11-14 22:13:21+00'::timestamp with time zone))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=144.03..168.30 rows=352 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=144.03..168.30 rows=352 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '2023-11-14 22:13:21+00'::timestamp with time zone))
                ->  Limit  (cost=100.16..104.18 rows=1 width=26)
                      ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_2  (cost=100.16..104.18 rows=1 width=26)
                            Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key > '3'::smallint))
                            -> Storage Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                Projections: priority_key, scheduled_at, run_key
                                -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                    Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key > '3'::smallint))
  ->  Nested Loop (Batched Join)  (cost=171.03..193.40 rows=64 width=402)
        Recheck Cond: (run_key = candidates.run_key)
        ->  CTE Scan on candidates  (cost=0.00..1.28 rows=64 width=26)
        ->  Index Only Scan using workflow_dispatch_pkey on workflow_dispatch intent  (cost=171.03..177.14 rows=64 width=376)
              Index Cond: (run_key = candidates.run_key)
              -> Storage Scan on workflow_dispatch_pkey  (cost=171.03..177.14 rows=64 width=376 loops=1)
                  Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, queue_key, deployment_key, build_key, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
                  -> B-Tree Scan on workflow_dispatch_pkey  (cost=171.03..177.14 rows=64 width=376 loops=1)
                      Index Cond: (run_key = candidates.run_key)

Queue mode 0, offset=3840, before=true, range selectivity=0.249558:
Limit  (cost=417.28..512.05 rows=1 width=172)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=417.28..512.05 rows=1 width=172)
        Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
        Filter: (ROW(priority_key, scheduled_at, run_key) > ROW('3'::smallint, '2023-11-14 22:13:23+00'::timestamp with time zone, '<fixture-uuid-4>'::uuid))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=417.28..512.05 rows=869 width=172 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=417.28..512.05 rows=869 width=172 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=417.28..512.05 rows=869 width=172 loops=1)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=417.28..512.05 rows=869 width=172 loops=1)

Queue mode 0, offset=3840, before=false, range selectivity=0.249558:
Sort  (cost=455.48..455.55 rows=27 width=402)
  Sort Key: candidates.priority_key, candidates.scheduled_at, intent.run_key
  CTE candidates
    ->  Limit  (cost=312.76..317.20 rows=27 width=26)
          ->  Merge Append  (cost=312.76..317.20 rows=27 width=26)
                Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                ->  Sort  (cost=108.39..108.45 rows=25 width=26)
                      Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                      ->  Limit  (cost=103.16..107.81 rows=25 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=103.16..107.81 rows=25 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '2023-11-14 22:13:23+00'::timestamp with time zone) AND (run_key > '<fixture-uuid-4>'::uuid))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=103.16..107.81 rows=25 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=103.16..107.81 rows=25 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '2023-11-14 22:13:23+00'::timestamp with time zone) AND (run_key > '<fixture-uuid-4>'::uuid))
                ->  Sort  (cost=104.19..104.20 rows=1 width=26)
                      Sort Key: workflow_dispatch_1.priority_key, workflow_dispatch_1.scheduled_at, workflow_dispatch_1.run_key
                      ->  Limit  (cost=100.16..104.18 rows=1 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_1  (cost=100.16..104.18 rows=1 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '2023-11-14 22:13:23+00'::timestamp with time zone))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '2023-11-14 22:13:23+00'::timestamp with time zone))
                ->  Limit  (cost=100.16..104.18 rows=1 width=26)
                      ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_2  (cost=100.16..104.18 rows=1 width=26)
                            Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key > '3'::smallint))
                            -> Storage Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                Projections: priority_key, scheduled_at, run_key
                                -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                    Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key > '3'::smallint))
  ->  Nested Loop (Batched Join)  (cost=129.41..137.64 rows=27 width=402)
        Recheck Cond: (run_key = candidates.run_key)
        ->  CTE Scan on candidates  (cost=0.00..0.54 rows=27 width=26)
        ->  Index Only Scan using workflow_dispatch_pkey on workflow_dispatch intent  (cost=129.41..134.28 rows=27 width=376)
              Index Cond: (run_key = candidates.run_key)
              -> Storage Scan on workflow_dispatch_pkey  (cost=129.41..134.28 rows=27 width=376 loops=1)
                  Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, queue_key, deployment_key, build_key, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
                  -> B-Tree Scan on workflow_dispatch_pkey  (cost=129.41..134.28 rows=27 width=376 loops=1)
                      Index Cond: (run_key = candidates.run_key)

Queue mode 1, offset=0, before=true, range selectivity=0.249558:
Limit  (cost=412.53..419.15 rows=64 width=172)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=412.53..500.43 rows=850 width=172)
        Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=412.53..500.43 rows=850 width=172 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=412.53..500.43 rows=850 width=172 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=412.53..500.43 rows=850 width=172 loops=1)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=412.53..500.43 rows=850 width=172 loops=1)

Queue mode 1, offset=0, before=false, range selectivity=0.249558:
Sort  (cost=405.50..405.66 rows=64 width=402)
  Sort Key: candidates.priority_key, candidates.scheduled_at, intent.run_key
  CTE candidates
    ->  Limit  (cost=206.28..210.18 rows=64 width=26)
          ->  Limit  (cost=206.28..210.18 rows=64 width=26)
                ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=206.28..258.01 rows=850 width=26)
                      Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
                      -> Storage Scan on idx_workflow_dispatch_queue  (cost=206.28..258.01 rows=850 width=26 loops=1)
                          Projections: priority_key, scheduled_at, run_key
                          -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=206.28..258.01 rows=850 width=26 loops=1)
                              Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
  ->  Nested Loop (Batched Join)  (cost=171.03..193.40 rows=64 width=402)
        Recheck Cond: (run_key = candidates.run_key)
        ->  CTE Scan on candidates  (cost=0.00..1.28 rows=64 width=26)
        ->  Index Only Scan using workflow_dispatch_pkey on workflow_dispatch intent  (cost=171.03..177.14 rows=64 width=376)
              Index Cond: (run_key = candidates.run_key)
              -> Storage Scan on workflow_dispatch_pkey  (cost=171.03..177.14 rows=64 width=376 loops=1)
                  Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, queue_key, deployment_key, build_key, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
                  -> B-Tree Scan on workflow_dispatch_pkey  (cost=171.03..177.14 rows=64 width=376 loops=1)
                      Index Cond: (run_key = candidates.run_key)

Queue mode 1, offset=768, before=true, range selectivity=0.249558:
Limit  (cost=412.53..506.80 rows=1 width=172)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=412.53..506.80 rows=1 width=172)
        Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
        Filter: (ROW(priority_key, scheduled_at, run_key) > ROW('3'::smallint, '2023-11-14 22:13:20+00'::timestamp with time zone, '<fixture-uuid-5>'::uuid))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=412.53..506.80 rows=850 width=172 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=412.53..506.80 rows=850 width=172 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=412.53..506.80 rows=850 width=172 loops=1)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=412.53..506.80 rows=850 width=172 loops=1)

Queue mode 1, offset=768, before=false, range selectivity=0.249558:
Sort  (cost=594.39..594.55 rows=64 width=402)
  Sort Key: candidates.priority_key, candidates.scheduled_at, intent.run_key
  CTE candidates
    ->  Limit  (cost=396.09..399.07 rows=64 width=26)
          ->  Merge Append  (cost=396.09..402.10 rows=129 width=26)
                Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                ->  Sort  (cost=115.62..115.78 rows=64 width=26)
                      Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                      ->  Limit  (cost=108.03..113.70 rows=64 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=108.03..113.70 rows=64 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '2023-11-14 22:13:20+00'::timestamp with time zone) AND (run_key > '<fixture-uuid-5>'::uuid))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=108.03..113.70 rows=64 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=108.03..113.70 rows=64 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '2023-11-14 22:13:20+00'::timestamp with time zone) AND (run_key > '<fixture-uuid-5>'::uuid))
                ->  Sort  (cost=180.29..180.45 rows=64 width=26)
                      Sort Key: workflow_dispatch_1.priority_key, workflow_dispatch_1.scheduled_at, workflow_dispatch_1.run_key
                      ->  Limit  (cost=174.28..178.37 rows=64 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_1  (cost=174.28..212.24 rows=594 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '2023-11-14 22:13:20+00'::timestamp with time zone))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=174.28..212.24 rows=594 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=174.28..212.24 rows=594 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '2023-11-14 22:13:20+00'::timestamp with time zone))
                ->  Limit  (cost=100.16..104.18 rows=1 width=26)
                      ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_2  (cost=100.16..104.18 rows=1 width=26)
                            Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key > '3'::smallint))
                            -> Storage Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                Projections: priority_key, scheduled_at, run_key
                                -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                    Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key > '3'::smallint))
  ->  Nested Loop (Batched Join)  (cost=171.03..193.40 rows=64 width=402)
        Recheck Cond: (run_key = candidates.run_key)
        ->  CTE Scan on candidates  (cost=0.00..1.28 rows=64 width=26)
        ->  Index Only Scan using workflow_dispatch_pkey on workflow_dispatch intent  (cost=171.03..177.14 rows=64 width=376)
              Index Cond: (run_key = candidates.run_key)
              -> Storage Scan on workflow_dispatch_pkey  (cost=171.03..177.14 rows=64 width=376 loops=1)
                  Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, queue_key, deployment_key, build_key, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
                  -> B-Tree Scan on workflow_dispatch_pkey  (cost=171.03..177.14 rows=64 width=376 loops=1)
                      Index Cond: (run_key = candidates.run_key)

Queue mode 1, offset=2048, before=true, range selectivity=0.249558:
Limit  (cost=412.53..506.80 rows=1 width=172)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=412.53..506.80 rows=1 width=172)
        Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
        Filter: (ROW(priority_key, scheduled_at, run_key) > ROW('3'::smallint, '2023-11-14 22:13:21+00'::timestamp with time zone, '<fixture-uuid-6>'::uuid))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=412.53..506.80 rows=850 width=172 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=412.53..506.80 rows=850 width=172 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=412.53..506.80 rows=850 width=172 loops=1)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=412.53..506.80 rows=850 width=172 loops=1)

Queue mode 1, offset=2048, before=false, range selectivity=0.249558:
Sort  (cost=554.01..554.17 rows=64 width=402)
  Sort Key: candidates.priority_key, candidates.scheduled_at, intent.run_key
  CTE candidates
    ->  Limit  (cost=353.81..358.69 rows=64 width=26)
          ->  Merge Append  (cost=353.81..358.85 rows=66 width=26)
                Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                ->  Sort  (cost=104.19..104.20 rows=1 width=26)
                      Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                      ->  Limit  (cost=100.16..104.18 rows=1 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=100.16..104.18 rows=1 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '2023-11-14 22:13:21+00'::timestamp with time zone) AND (run_key > '<fixture-uuid-6>'::uuid))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '2023-11-14 22:13:21+00'::timestamp with time zone) AND (run_key > '<fixture-uuid-6>'::uuid))
                ->  Sort  (cost=149.43..149.59 rows=64 width=26)
                      Sort Key: workflow_dispatch_1.priority_key, workflow_dispatch_1.scheduled_at, workflow_dispatch_1.run_key
                      ->  Limit  (cost=143.03..147.51 rows=64 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_1  (cost=143.03..167.12 rows=344 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '2023-11-14 22:13:21+00'::timestamp with time zone))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=143.03..167.12 rows=344 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=143.03..167.12 rows=344 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '2023-11-14 22:13:21+00'::timestamp with time zone))
                ->  Limit  (cost=100.16..104.18 rows=1 width=26)
                      ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_2  (cost=100.16..104.18 rows=1 width=26)
                            Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key > '3'::smallint))
                            -> Storage Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                Projections: priority_key, scheduled_at, run_key
                                -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                    Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key > '3'::smallint))
  ->  Nested Loop (Batched Join)  (cost=171.03..193.40 rows=64 width=402)
        Recheck Cond: (run_key = candidates.run_key)
        ->  CTE Scan on candidates  (cost=0.00..1.28 rows=64 width=26)
        ->  Index Only Scan using workflow_dispatch_pkey on workflow_dispatch intent  (cost=171.03..177.14 rows=64 width=376)
              Index Cond: (run_key = candidates.run_key)
              -> Storage Scan on workflow_dispatch_pkey  (cost=171.03..177.14 rows=64 width=376 loops=1)
                  Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, queue_key, deployment_key, build_key, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
                  -> B-Tree Scan on workflow_dispatch_pkey  (cost=171.03..177.14 rows=64 width=376 loops=1)
                      Index Cond: (run_key = candidates.run_key)

Queue mode 1, offset=3840, before=true, range selectivity=0.249558:
Limit  (cost=412.53..506.80 rows=1 width=172)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=412.53..506.80 rows=1 width=172)
        Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
        Filter: (ROW(priority_key, scheduled_at, run_key) > ROW('3'::smallint, '2023-11-14 22:13:23+00'::timestamp with time zone, '<fixture-uuid-7>'::uuid))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=412.53..506.80 rows=850 width=172 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=412.53..506.80 rows=850 width=172 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=412.53..506.80 rows=850 width=172 loops=1)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=412.53..506.80 rows=850 width=172 loops=1)

Queue mode 1, offset=3840, before=false, range selectivity=0.249558:
Sort  (cost=455.48..455.55 rows=27 width=402)
  Sort Key: candidates.priority_key, candidates.scheduled_at, intent.run_key
  CTE candidates
    ->  Limit  (cost=312.76..317.20 rows=27 width=26)
          ->  Merge Append  (cost=312.76..317.20 rows=27 width=26)
                Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                ->  Sort  (cost=108.39..108.45 rows=25 width=26)
                      Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                      ->  Limit  (cost=103.16..107.81 rows=25 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=103.16..107.81 rows=25 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '2023-11-14 22:13:23+00'::timestamp with time zone) AND (run_key > '<fixture-uuid-7>'::uuid))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=103.16..107.81 rows=25 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=103.16..107.81 rows=25 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '2023-11-14 22:13:23+00'::timestamp with time zone) AND (run_key > '<fixture-uuid-7>'::uuid))
                ->  Sort  (cost=104.19..104.20 rows=1 width=26)
                      Sort Key: workflow_dispatch_1.priority_key, workflow_dispatch_1.scheduled_at, workflow_dispatch_1.run_key
                      ->  Limit  (cost=100.16..104.18 rows=1 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_1  (cost=100.16..104.18 rows=1 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '2023-11-14 22:13:23+00'::timestamp with time zone))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '2023-11-14 22:13:23+00'::timestamp with time zone))
                ->  Limit  (cost=100.16..104.18 rows=1 width=26)
                      ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_2  (cost=100.16..104.18 rows=1 width=26)
                            Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key > '3'::smallint))
                            -> Storage Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                Projections: priority_key, scheduled_at, run_key
                                -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                    Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key > '3'::smallint))
  ->  Nested Loop (Batched Join)  (cost=129.41..137.64 rows=27 width=402)
        Recheck Cond: (run_key = candidates.run_key)
        ->  CTE Scan on candidates  (cost=0.00..0.54 rows=27 width=26)
        ->  Index Only Scan using workflow_dispatch_pkey on workflow_dispatch intent  (cost=129.41..134.28 rows=27 width=376)
              Index Cond: (run_key = candidates.run_key)
              -> Storage Scan on workflow_dispatch_pkey  (cost=129.41..134.28 rows=27 width=376 loops=1)
                  Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, queue_key, deployment_key, build_key, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
                  -> B-Tree Scan on workflow_dispatch_pkey  (cost=129.41..134.28 rows=27 width=376 loops=1)
                      Index Cond: (run_key = candidates.run_key)

Home continuation=false, matching rows=284, selectivity=0.017303:
Limit  (cost=135.53..139.68 rows=64 width=16)
  ->  Index Only Scan using idx_workflow_dispatch_home on workflow_dispatch  (cost=135.53..153.95 rows=284 width=16)
        Index Cond: (shard_id = '<fixture-uuid-8>'::uuid)
        -> Storage Scan on idx_workflow_dispatch_home  (cost=135.53..153.95 rows=284 width=16 loops=1)
            Projections: run_key
            -> B-Tree Scan on idx_workflow_dispatch_home  (cost=135.53..153.95 rows=284 width=16 loops=1)
                Index Cond: (shard_id = '<fixture-uuid-8>'::uuid)

Home continuation=true, matching rows=284, selectivity=0.017303:
Limit  (cost=135.41..139.73 rows=64 width=16)
  ->  Index Only Scan using idx_workflow_dispatch_home on workflow_dispatch  (cost=135.41..154.52 rows=283 width=16)
        Index Cond: ((shard_id = '<fixture-uuid-8>'::uuid) AND (run_key > '<fixture-uuid-9>'::uuid))
        -> Storage Scan on idx_workflow_dispatch_home  (cost=135.41..154.52 rows=283 width=16 loops=1)
            Projections: run_key
            -> B-Tree Scan on idx_workflow_dispatch_home  (cost=135.41..154.52 rows=283 width=16 loops=1)
                Index Cond: ((shard_id = '<fixture-uuid-8>'::uuid) AND (run_key > '<fixture-uuid-9>'::uuid))
```
