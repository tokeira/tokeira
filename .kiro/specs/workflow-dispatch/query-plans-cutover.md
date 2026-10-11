# Workflow dispatch cutover: query and placement plans

Captured through SQLx on real Aurora DSQL using the repository query builders
at implementation revision `ba2d7654dba8e130279e36f853491c430a364ba4`.
Fixture UUIDs, digests and timestamp literals are replaced consistently; index
names, predicates, costs and cardinalities are unchanged. `EXPLAIN` does not
measure storage-engine rows examined. Reported traversal counts and durations
come from executing the actual page APIs, separately from the plans.

See [implementation evidence](implementation-evidence.md) for the validation
outcomes, shared-cluster counts and scope of these observations.

## Workflow queue and home pages

```text
Seeded rows: 16384; table rows after seeding: 17806.
Live and Exact each contain 4096 rows in queue 0, all priority 3, with 1024 equal timestamps per group. Another 8192 rows span 128 unrelated queue families.
64 execution homes contain 256 seeded rows each.
ASYNC indexes were awaited by the migration runner before seeding.

Queue mode 0, offset=0, before=true, range selectivity=0.230035:
Limit  (cost=504.04..515.55 rows=64 width=168)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=504.04..722.77 rows=1216 width=168)
        Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=504.04..722.77 rows=1216 width=168 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=504.04..722.77 rows=1216 width=168 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=504.04..722.77 rows=608 width=168 loops=2)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=504.04..722.77 rows=608 width=168 loops=1)

Queue mode 0, offset=0, before=false, range selectivity=0.230035:
Sort  (cost=451.23..451.39 rows=64 width=398)
  Sort Key: candidates.priority_key, candidates.scheduled_at, intent.run_key
  CTE candidates
    ->  Limit  (cost=252.03..255.75 rows=64 width=26)
          ->  Limit  (cost=252.03..255.75 rows=64 width=26)
                ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=252.03..322.53 rows=1216 width=26)
                      Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
                      -> Storage Scan on idx_workflow_dispatch_queue  (cost=252.03..322.53 rows=1216 width=26 loops=1)
                          Projections: priority_key, scheduled_at, run_key
                          -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=252.03..322.53 rows=1216 width=26 loops=1)
                              Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
  ->  Nested Loop (Batched Join)  (cost=171.03..193.56 rows=64 width=398)
        Recheck Cond: (run_key = candidates.run_key)
        ->  CTE Scan on candidates  (cost=0.00..1.28 rows=64 width=26)
        ->  Index Only Scan using workflow_dispatch_pkey on workflow_dispatch intent  (cost=171.03..177.30 rows=64 width=372)
              Index Cond: (run_key = candidates.run_key)
              -> Storage Scan on workflow_dispatch_pkey  (cost=171.03..177.30 rows=64 width=372 loops=1)
                  Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, queue_key, deployment_key, build_key, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
                  -> B-Tree Scan on workflow_dispatch_pkey  (cost=171.03..177.30 rows=64 width=372 loops=1)
                      Index Cond: (run_key = candidates.run_key)

Queue mode 0, offset=768, before=true, range selectivity=0.230035:
Limit  (cost=504.04..731.89 rows=54 width=168)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=504.04..731.89 rows=54 width=168)
        Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
        Filter: (ROW(priority_key, scheduled_at, run_key) > ROW('3'::smallint, '<fixture-time-1>'::timestamp with time zone, '<fixture-uuid-2>'::uuid))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=504.04..731.89 rows=1216 width=168 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=504.04..731.89 rows=1216 width=168 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=504.04..731.89 rows=608 width=168 loops=2)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=504.04..731.89 rows=608 width=168 loops=1)

Queue mode 0, offset=768, before=false, range selectivity=0.230035:
Sort  (cost=609.23..609.39 rows=64 width=398)
  Sort Key: candidates.priority_key, candidates.scheduled_at, intent.run_key
  CTE candidates
    ->  Limit  (cost=411.00..413.75 rows=64 width=26)
          ->  Merge Append  (cost=411.00..418.81 rows=182 width=26)
                Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                ->  Sort  (cost=115.88..116.04 rows=64 width=26)
                      Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                      ->  Limit  (cost=108.53..113.96 rows=64 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=108.53..114.30 rows=68 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '<fixture-time-1>'::timestamp with time zone) AND (run_key > '<fixture-uuid-2>'::uuid))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=108.53..114.30 rows=68 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=108.53..114.30 rows=68 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '<fixture-time-1>'::timestamp with time zone) AND (run_key > '<fixture-uuid-2>'::uuid))
                ->  Sort  (cost=188.31..188.47 rows=64 width=26)
                      Sort Key: workflow_dispatch_1.priority_key, workflow_dispatch_1.scheduled_at, workflow_dispatch_1.run_key
                      ->  Limit  (cost=182.16..186.39 rows=64 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_1  (cost=182.16..225.60 rows=657 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '<fixture-time-1>'::timestamp with time zone))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=182.16..225.60 rows=657 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=182.16..225.60 rows=657 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '<fixture-time-1>'::timestamp with time zone))
                ->  Limit  (cost=106.78..111.92 rows=54 width=26)
                      ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_2  (cost=106.78..111.92 rows=54 width=26)
                            Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key > '3'::smallint))
                            -> Storage Scan on idx_workflow_dispatch_queue  (cost=106.78..111.92 rows=54 width=26 loops=1)
                                Projections: priority_key, scheduled_at, run_key
                                -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=106.78..111.92 rows=54 width=26 loops=1)
                                    Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key > '3'::smallint))
  ->  Nested Loop (Batched Join)  (cost=171.03..193.56 rows=64 width=398)
        Recheck Cond: (run_key = candidates.run_key)
        ->  CTE Scan on candidates  (cost=0.00..1.28 rows=64 width=26)
        ->  Index Only Scan using workflow_dispatch_pkey on workflow_dispatch intent  (cost=171.03..177.30 rows=64 width=372)
              Index Cond: (run_key = candidates.run_key)
              -> Storage Scan on workflow_dispatch_pkey  (cost=171.03..177.30 rows=64 width=372 loops=1)
                  Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, queue_key, deployment_key, build_key, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
                  -> B-Tree Scan on workflow_dispatch_pkey  (cost=171.03..177.30 rows=64 width=372 loops=1)
                      Index Cond: (run_key = candidates.run_key)

Queue mode 0, offset=2048, before=true, range selectivity=0.230035:
Limit  (cost=504.04..731.89 rows=54 width=168)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=504.04..731.89 rows=54 width=168)
        Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
        Filter: (ROW(priority_key, scheduled_at, run_key) > ROW('3'::smallint, '<fixture-time-2>'::timestamp with time zone, '<fixture-uuid-3>'::uuid))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=504.04..731.89 rows=1216 width=168 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=504.04..731.89 rows=1216 width=168 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=504.04..731.89 rows=608 width=168 loops=2)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=504.04..731.89 rows=608 width=168 loops=1)

Queue mode 0, offset=2048, before=false, range selectivity=0.230035:
Sort  (cost=566.89..567.05 rows=64 width=398)
  Sort Key: candidates.priority_key, candidates.scheduled_at, intent.run_key
  CTE candidates
    ->  Limit  (cost=367.74..371.41 rows=64 width=26)
          ->  Merge Append  (cost=367.74..374.57 rows=119 width=26)
                Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                ->  Sort  (cost=104.20..104.20 rows=1 width=26)
                      Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                      ->  Limit  (cost=100.16..104.19 rows=1 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=100.16..104.19 rows=1 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '<fixture-time-2>'::timestamp with time zone) AND (run_key > '<fixture-uuid-3>'::uuid))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=100.16..104.19 rows=1 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=100.16..104.19 rows=1 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '<fixture-time-2>'::timestamp with time zone) AND (run_key > '<fixture-uuid-3>'::uuid))
                ->  Sort  (cost=156.73..156.89 rows=64 width=26)
                      Sort Key: workflow_dispatch_1.priority_key, workflow_dispatch_1.scheduled_at, workflow_dispatch_1.run_key
                      ->  Limit  (cost=150.78..154.81 rows=64 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_1  (cost=150.78..176.33 rows=406 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '<fixture-time-2>'::timestamp with time zone))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=150.78..176.33 rows=406 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=150.78..176.33 rows=406 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '<fixture-time-2>'::timestamp with time zone))
                ->  Limit  (cost=106.78..111.92 rows=54 width=26)
                      ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_2  (cost=106.78..111.92 rows=54 width=26)
                            Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key > '3'::smallint))
                            -> Storage Scan on idx_workflow_dispatch_queue  (cost=106.78..111.92 rows=54 width=26 loops=1)
                                Projections: priority_key, scheduled_at, run_key
                                -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=106.78..111.92 rows=54 width=26 loops=1)
                                    Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key > '3'::smallint))
  ->  Nested Loop (Batched Join)  (cost=171.03..193.56 rows=64 width=398)
        Recheck Cond: (run_key = candidates.run_key)
        ->  CTE Scan on candidates  (cost=0.00..1.28 rows=64 width=26)
        ->  Index Only Scan using workflow_dispatch_pkey on workflow_dispatch intent  (cost=171.03..177.30 rows=64 width=372)
              Index Cond: (run_key = candidates.run_key)
              -> Storage Scan on workflow_dispatch_pkey  (cost=171.03..177.30 rows=64 width=372 loops=1)
                  Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, queue_key, deployment_key, build_key, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
                  -> B-Tree Scan on workflow_dispatch_pkey  (cost=171.03..177.30 rows=64 width=372 loops=1)
                      Index Cond: (run_key = candidates.run_key)

Queue mode 0, offset=3840, before=true, range selectivity=0.230035:
Limit  (cost=504.04..731.89 rows=54 width=168)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=504.04..731.89 rows=54 width=168)
        Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
        Filter: (ROW(priority_key, scheduled_at, run_key) > ROW('3'::smallint, '<fixture-time-3>'::timestamp with time zone, '<fixture-uuid-4>'::uuid))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=504.04..731.89 rows=1216 width=168 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=504.04..731.89 rows=1216 width=168 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=504.04..731.89 rows=608 width=168 loops=2)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=504.04..731.89 rows=608 width=168 loops=1)

Queue mode 0, offset=3840, before=false, range selectivity=0.230035:
Sort  (cost=521.98..522.14 rows=64 width=398)
  Sort Key: candidates.priority_key, candidates.scheduled_at, intent.run_key
  CTE candidates
    ->  Limit  (cost=322.14..326.50 rows=64 width=26)
          ->  Merge Append  (cost=322.14..328.61 rows=95 width=26)
                Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                ->  Sort  (cost=111.14..111.24 rows=40 width=26)
                      Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                      ->  Limit  (cost=105.03..110.08 rows=40 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=105.03..110.08 rows=40 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '<fixture-time-3>'::timestamp with time zone) AND (run_key > '<fixture-uuid-4>'::uuid))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=105.03..110.08 rows=40 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=105.03..110.08 rows=40 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '<fixture-time-3>'::timestamp with time zone) AND (run_key > '<fixture-uuid-4>'::uuid))
                ->  Sort  (cost=104.19..104.20 rows=1 width=26)
                      Sort Key: workflow_dispatch_1.priority_key, workflow_dispatch_1.scheduled_at, workflow_dispatch_1.run_key
                      ->  Limit  (cost=100.16..104.18 rows=1 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_1  (cost=100.16..104.18 rows=1 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '<fixture-time-3>'::timestamp with time zone))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '<fixture-time-3>'::timestamp with time zone))
                ->  Limit  (cost=106.78..111.92 rows=54 width=26)
                      ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_2  (cost=106.78..111.92 rows=54 width=26)
                            Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key > '3'::smallint))
                            -> Storage Scan on idx_workflow_dispatch_queue  (cost=106.78..111.92 rows=54 width=26 loops=1)
                                Projections: priority_key, scheduled_at, run_key
                                -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=106.78..111.92 rows=54 width=26 loops=1)
                                    Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '0'::smallint) AND (deployment_key = '<fixture-digest-2>'::text) AND (build_key = '<fixture-digest-3>'::text) AND (priority_key > '3'::smallint))
  ->  Nested Loop (Batched Join)  (cost=171.03..193.56 rows=64 width=398)
        Recheck Cond: (run_key = candidates.run_key)
        ->  CTE Scan on candidates  (cost=0.00..1.28 rows=64 width=26)
        ->  Index Only Scan using workflow_dispatch_pkey on workflow_dispatch intent  (cost=171.03..177.30 rows=64 width=372)
              Index Cond: (run_key = candidates.run_key)
              -> Storage Scan on workflow_dispatch_pkey  (cost=171.03..177.30 rows=64 width=372 loops=1)
                  Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, queue_key, deployment_key, build_key, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
                  -> B-Tree Scan on workflow_dispatch_pkey  (cost=171.03..177.30 rows=64 width=372 loops=1)
                      Index Cond: (run_key = candidates.run_key)

Queue mode 0 complete pass: 4096 returned rows, 65 page SQL statements, 5.279 seconds. Storage-engine rows examined are not measured by EXPLAIN.

Queue mode 1, offset=0, before=true, range selectivity=0.230035:
Limit  (cost=315.28..322.03 rows=64 width=168)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=315.28..363.91 rows=461 width=168)
        Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=315.28..363.91 rows=461 width=168 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=315.28..363.91 rows=461 width=168 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=315.28..363.91 rows=461 width=168 loops=1)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=315.28..363.91 rows=461 width=168 loops=1)

Queue mode 1, offset=0, before=false, range selectivity=0.230035:
Sort  (cost=357.10..357.26 rows=64 width=398)
  Sort Key: candidates.priority_key, candidates.scheduled_at, intent.run_key
  CTE candidates
    ->  Limit  (cost=157.66..161.62 rows=64 width=26)
          ->  Limit  (cost=157.66..161.62 rows=64 width=26)
                ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=157.66..186.19 rows=461 width=26)
                      Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
                      -> Storage Scan on idx_workflow_dispatch_queue  (cost=157.66..186.19 rows=461 width=26 loops=1)
                          Projections: priority_key, scheduled_at, run_key
                          -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=157.66..186.19 rows=461 width=26 loops=1)
                              Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
  ->  Nested Loop (Batched Join)  (cost=171.03..193.56 rows=64 width=398)
        Recheck Cond: (run_key = candidates.run_key)
        ->  CTE Scan on candidates  (cost=0.00..1.28 rows=64 width=26)
        ->  Index Only Scan using workflow_dispatch_pkey on workflow_dispatch intent  (cost=171.03..177.30 rows=64 width=372)
              Index Cond: (run_key = candidates.run_key)
              -> Storage Scan on workflow_dispatch_pkey  (cost=171.03..177.30 rows=64 width=372 loops=1)
                  Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, queue_key, deployment_key, build_key, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
                  -> B-Tree Scan on workflow_dispatch_pkey  (cost=171.03..177.30 rows=64 width=372 loops=1)
                      Index Cond: (run_key = candidates.run_key)

Queue mode 1, offset=768, before=true, range selectivity=0.230035:
Limit  (cost=315.28..367.36 rows=21 width=168)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=315.28..367.36 rows=21 width=168)
        Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
        Filter: (ROW(priority_key, scheduled_at, run_key) > ROW('3'::smallint, '<fixture-time-1>'::timestamp with time zone, '<fixture-uuid-5>'::uuid))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=315.28..367.36 rows=461 width=168 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=315.28..367.36 rows=461 width=168 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=315.28..367.36 rows=461 width=168 loops=1)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=315.28..367.36 rows=461 width=168 loops=1)

Queue mode 1, offset=768, before=false, range selectivity=0.230035:
Sort  (cost=547.92..548.08 rows=64 width=398)
  Sort Key: candidates.priority_key, candidates.scheduled_at, intent.run_key
  CTE candidates
    ->  Limit  (cost=348.92..352.44 rows=64 width=26)
          ->  Merge Append  (cost=348.92..355.02 rows=111 width=26)
                Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                ->  Sort  (cost=108.57..108.64 rows=26 width=26)
                      Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                      ->  Limit  (cost=103.28..107.96 rows=26 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=103.28..107.96 rows=26 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '<fixture-time-1>'::timestamp with time zone) AND (run_key > '<fixture-uuid-5>'::uuid))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=103.28..107.96 rows=26 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=103.28..107.96 rows=26 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '<fixture-time-1>'::timestamp with time zone) AND (run_key > '<fixture-uuid-5>'::uuid))
                ->  Sort  (cost=137.67..137.83 rows=64 width=26)
                      Sort Key: workflow_dispatch_1.priority_key, workflow_dispatch_1.scheduled_at, workflow_dispatch_1.run_key
                      ->  Limit  (cost=131.16..135.75 rows=64 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_1  (cost=131.16..149.01 rows=249 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '<fixture-time-1>'::timestamp with time zone))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=131.16..149.01 rows=249 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=131.16..149.01 rows=249 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '<fixture-time-1>'::timestamp with time zone))
                ->  Limit  (cost=102.66..107.10 rows=21 width=26)
                      ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_2  (cost=102.66..107.10 rows=21 width=26)
                            Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key > '3'::smallint))
                            -> Storage Scan on idx_workflow_dispatch_queue  (cost=102.66..107.10 rows=21 width=26 loops=1)
                                Projections: priority_key, scheduled_at, run_key
                                -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=102.66..107.10 rows=21 width=26 loops=1)
                                    Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key > '3'::smallint))
  ->  Nested Loop (Batched Join)  (cost=171.03..193.56 rows=64 width=398)
        Recheck Cond: (run_key = candidates.run_key)
        ->  CTE Scan on candidates  (cost=0.00..1.28 rows=64 width=26)
        ->  Index Only Scan using workflow_dispatch_pkey on workflow_dispatch intent  (cost=171.03..177.30 rows=64 width=372)
              Index Cond: (run_key = candidates.run_key)
              -> Storage Scan on workflow_dispatch_pkey  (cost=171.03..177.30 rows=64 width=372 loops=1)
                  Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, queue_key, deployment_key, build_key, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
                  -> B-Tree Scan on workflow_dispatch_pkey  (cost=171.03..177.30 rows=64 width=372 loops=1)
                      Index Cond: (run_key = candidates.run_key)

Queue mode 1, offset=2048, before=true, range selectivity=0.230035:
Limit  (cost=315.28..367.36 rows=21 width=168)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=315.28..367.36 rows=21 width=168)
        Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
        Filter: (ROW(priority_key, scheduled_at, run_key) > ROW('3'::smallint, '<fixture-time-2>'::timestamp with time zone, '<fixture-uuid-6>'::uuid))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=315.28..367.36 rows=461 width=168 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=315.28..367.36 rows=461 width=168 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=315.28..367.36 rows=461 width=168 loops=1)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=315.28..367.36 rows=461 width=168 loops=1)

Queue mode 1, offset=2048, before=false, range selectivity=0.230035:
Sort  (cost=532.65..532.81 rows=64 width=398)
  Sort Key: candidates.priority_key, candidates.scheduled_at, intent.run_key
  CTE candidates
    ->  Limit  (cost=332.91..337.17 rows=64 width=26)
          ->  Merge Append  (cost=332.91..338.63 rows=86 width=26)
                Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                ->  Sort  (cost=104.20..104.20 rows=1 width=26)
                      Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                      ->  Limit  (cost=100.16..104.19 rows=1 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=100.16..104.19 rows=1 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '<fixture-time-2>'::timestamp with time zone) AND (run_key > '<fixture-uuid-6>'::uuid))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=100.16..104.19 rows=1 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=100.16..104.19 rows=1 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '<fixture-time-2>'::timestamp with time zone) AND (run_key > '<fixture-uuid-6>'::uuid))
                ->  Sort  (cost=126.03..126.19 rows=64 width=26)
                      Sort Key: workflow_dispatch_1.priority_key, workflow_dispatch_1.scheduled_at, workflow_dispatch_1.run_key
                      ->  Limit  (cost=119.28..124.11 rows=64 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_1  (cost=119.28..130.90 rows=154 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '<fixture-time-2>'::timestamp with time zone))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=119.28..130.90 rows=154 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=119.28..130.90 rows=154 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '<fixture-time-2>'::timestamp with time zone))
                ->  Limit  (cost=102.66..107.10 rows=21 width=26)
                      ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_2  (cost=102.66..107.10 rows=21 width=26)
                            Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key > '3'::smallint))
                            -> Storage Scan on idx_workflow_dispatch_queue  (cost=102.66..107.10 rows=21 width=26 loops=1)
                                Projections: priority_key, scheduled_at, run_key
                                -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=102.66..107.10 rows=21 width=26 loops=1)
                                    Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key > '3'::smallint))
  ->  Nested Loop (Batched Join)  (cost=171.03..193.56 rows=64 width=398)
        Recheck Cond: (run_key = candidates.run_key)
        ->  CTE Scan on candidates  (cost=0.00..1.28 rows=64 width=26)
        ->  Index Only Scan using workflow_dispatch_pkey on workflow_dispatch intent  (cost=171.03..177.30 rows=64 width=372)
              Index Cond: (run_key = candidates.run_key)
              -> Storage Scan on workflow_dispatch_pkey  (cost=171.03..177.30 rows=64 width=372 loops=1)
                  Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, queue_key, deployment_key, build_key, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
                  -> B-Tree Scan on workflow_dispatch_pkey  (cost=171.03..177.30 rows=64 width=372 loops=1)
                      Index Cond: (run_key = candidates.run_key)

Queue mode 1, offset=3840, before=true, range selectivity=0.230035:
Limit  (cost=315.28..367.36 rows=21 width=168)
  ->  Index Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=315.28..367.36 rows=21 width=168)
        Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
        Filter: (ROW(priority_key, scheduled_at, run_key) > ROW('3'::smallint, '<fixture-time-3>'::timestamp with time zone, '<fixture-uuid-7>'::uuid))
        -> Storage Scan on idx_workflow_dispatch_queue  (cost=315.28..367.36 rows=461 width=168 loops=1)
            -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=315.28..367.36 rows=461 width=168 loops=1)
                Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text))
        -> Storage Lookup on workflow_dispatch  (cost=315.28..367.36 rows=461 width=168 loops=1)
            Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
            -> B-Tree Lookup on workflow_dispatch  (cost=315.28..367.36 rows=461 width=168 loops=1)

Queue mode 1, offset=3840, before=false, range selectivity=0.230035:
Sort  (cost=471.25..471.35 rows=37 width=398)
  Sort Key: candidates.priority_key, candidates.scheduled_at, intent.run_key
  CTE candidates
    ->  Limit  (cost=313.47..318.43 rows=37 width=26)
          ->  Merge Append  (cost=313.47..318.43 rows=37 width=26)
                Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                ->  Sort  (cost=106.59..106.63 rows=15 width=26)
                      Sort Key: workflow_dispatch.priority_key, workflow_dispatch.scheduled_at, workflow_dispatch.run_key
                      ->  Limit  (cost=101.91..106.30 rows=15 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch  (cost=101.91..106.30 rows=15 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '<fixture-time-3>'::timestamp with time zone) AND (run_key > '<fixture-uuid-7>'::uuid))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=101.91..106.30 rows=15 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=101.91..106.30 rows=15 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at = '<fixture-time-3>'::timestamp with time zone) AND (run_key > '<fixture-uuid-7>'::uuid))
                ->  Sort  (cost=104.19..104.20 rows=1 width=26)
                      Sort Key: workflow_dispatch_1.priority_key, workflow_dispatch_1.scheduled_at, workflow_dispatch_1.run_key
                      ->  Limit  (cost=100.16..104.18 rows=1 width=26)
                            ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_1  (cost=100.16..104.18 rows=1 width=26)
                                  Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '<fixture-time-3>'::timestamp with time zone))
                                  -> Storage Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                      Projections: priority_key, scheduled_at, run_key
                                      -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=100.16..104.18 rows=1 width=26 loops=1)
                                          Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key = '3'::smallint) AND (scheduled_at > '<fixture-time-3>'::timestamp with time zone))
                ->  Limit  (cost=102.66..107.10 rows=21 width=26)
                      ->  Index Only Scan using idx_workflow_dispatch_queue on workflow_dispatch workflow_dispatch_2  (cost=102.66..107.10 rows=21 width=26)
                            Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key > '3'::smallint))
                            -> Storage Scan on idx_workflow_dispatch_queue  (cost=102.66..107.10 rows=21 width=26 loops=1)
                                Projections: priority_key, scheduled_at, run_key
                                -> B-Tree Scan on idx_workflow_dispatch_queue  (cost=102.66..107.10 rows=21 width=26 loops=1)
                                    Index Cond: ((queue_namespace = '<fixture-uuid-1>'::uuid) AND (queue_key = '<fixture-digest-1>'::text) AND (routing_mode = '1'::smallint) AND (deployment_key = '<fixture-digest-4>'::text) AND (build_key = '<fixture-digest-5>'::text) AND (priority_key > '3'::smallint))
  ->  Nested Loop (Batched Join)  (cost=140.66..151.86 rows=37 width=398)
        Recheck Cond: (run_key = candidates.run_key)
        ->  CTE Scan on candidates  (cost=0.00..0.74 rows=37 width=26)
        ->  Index Only Scan using workflow_dispatch_pkey on workflow_dispatch intent  (cost=140.66..145.96 rows=37 width=372)
              Index Cond: (run_key = candidates.run_key)
              -> Storage Scan on workflow_dispatch_pkey  (cost=140.66..145.96 rows=37 width=372 loops=1)
                  Projections: run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky, routing_mode, deployment, build_id, queue_key, deployment_key, build_key, logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
                  -> B-Tree Scan on workflow_dispatch_pkey  (cost=140.66..145.96 rows=37 width=372 loops=1)
                      Index Cond: (run_key = candidates.run_key)

Queue mode 1 complete pass: 4096 returned rows, 65 page SQL statements, 5.490 seconds. Storage-engine rows examined are not measured by EXPLAIN.

Home continuation=false, matching rows=487, selectivity=0.027350:
Limit  (cost=160.91..165.13 rows=64 width=16)
  ->  Index Only Scan using idx_workflow_dispatch_home on workflow_dispatch  (cost=160.91..193.05 rows=487 width=16)
        Index Cond: (shard_id = '<fixture-uuid-8>'::uuid)
        -> Storage Scan on idx_workflow_dispatch_home  (cost=160.91..193.05 rows=487 width=16 loops=1)
            Projections: run_key
            -> B-Tree Scan on idx_workflow_dispatch_home  (cost=160.91..193.05 rows=487 width=16 loops=1)
                Index Cond: (shard_id = '<fixture-uuid-8>'::uuid)

Home continuation=true, matching rows=487, selectivity=0.027350:
Limit  (cost=160.66..165.06 rows=64 width=16)
  ->  Index Only Scan using idx_workflow_dispatch_home on workflow_dispatch  (cost=160.66..194.00 rows=485 width=16)
        Index Cond: ((shard_id = '<fixture-uuid-8>'::uuid) AND (run_key > '<fixture-uuid-9>'::uuid))
        -> Storage Scan on idx_workflow_dispatch_home  (cost=160.66..194.00 rows=485 width=16 loops=1)
            Projections: run_key
            -> B-Tree Scan on idx_workflow_dispatch_home  (cost=160.66..194.00 rows=485 width=16 loops=1)
                Index Cond: ((shard_id = '<fixture-uuid-8>'::uuid) AND (run_key > '<fixture-uuid-9>'::uuid))
```

## Placement screening and joined timer pages

```text
Screening sample: 12,288 hot rows and 12,288 timers, all correctly placed; payloads are deliberately undecodable to prove screening reads only metadata.

Joined timer continuation offset 0:
Sort  (cost=230.29..230.54 rows=100 width=145)
  Sort Key: t.shard_id, t.fire_at, t.run_key, t.timer_id
  CTE timer_page
    ->  Limit  (cost=112.52..117.12 rows=100 width=59)
          ->  Index Only Scan using timer_bucket_pkey on timer_bucket  (cost=112.52..117.12 rows=100 width=59)
                -> Storage Scan on timer_bucket_pkey  (cost=112.52..117.12 rows=100 width=59 loops=1)
                    Projections: shard_id, fire_at, run_key, timer_id
                    -> B-Tree Scan on timer_bucket_pkey  (cost=112.52..117.12 rows=100 width=59 loops=1)
  InitPlan 2 (returns $1)
    ->  CTE Scan on timer_page  (cost=0.00..2.00 rows=100 width=16)
  ->  Hash Left Join  (cost=105.39..107.85 rows=100 width=145)
        Hash Cond: (t.run_key = workflow_hot.run_key)
        ->  CTE Scan on timer_page t  (cost=0.00..2.00 rows=100 width=72)
        ->  Hash  (cost=105.27..105.27 rows=9 width=89)
              ->  Index Only Scan using workflow_hot_pkey on workflow_hot  (cost=101.26..105.27 rows=9 width=89)
                    Index Cond: (run_key = ANY ($1))
                    -> Storage Scan on workflow_hot_pkey  (cost=101.26..105.27 rows=10 width=89 loops=1)
                        Projections: run_key, namespace_id, workflow_id
                        -> B-Tree Scan on workflow_hot_pkey  (cost=101.26..105.27 rows=10 width=89 loops=1)
                            Index Cond: (run_key = ANY ($1))

Joined timer continuation offset 4096:
Sort  (cost=540.64..540.85 rows=81 width=145)
  Sort Key: t.shard_id, t.fire_at, t.run_key, t.timer_id
  CTE timer_page
    ->  Limit  (cost=422.60..428.43 rows=81 width=59)
          ->  Merge Append  (cost=422.60..428.43 rows=81 width=59)
                Sort Key: timer_bucket.shard_id, timer_bucket.fire_at, timer_bucket.run_key, timer_bucket.timer_id
                ->  Sort  (cost=104.17..104.17 rows=1 width=59)
                      Sort Key: timer_bucket.shard_id, timer_bucket.fire_at, timer_bucket.run_key, timer_bucket.timer_id
                      ->  Limit  (cost=100.14..104.16 rows=1 width=59)
                            ->  Index Only Scan using timer_bucket_pkey on timer_bucket  (cost=100.14..104.16 rows=1 width=59)
                                  Index Cond: ((shard_id = '<fixture-uuid-10>'::uuid) AND (fire_at = '<fixture-time-4>'::timestamp with time zone) AND (run_key = '<fixture-uuid-11>'::uuid) AND (timer_id > 'screening'::text))
                                  -> Storage Scan on timer_bucket_pkey  (cost=100.14..104.16 rows=1 width=59 loops=1)
                                      Projections: shard_id, fire_at, run_key, timer_id
                                      -> B-Tree Scan on timer_bucket_pkey  (cost=100.14..104.16 rows=1 width=59 loops=1)
                                          Index Cond: ((shard_id = '<fixture-uuid-10>'::uuid) AND (fire_at = '<fixture-time-4>'::timestamp with time zone) AND (run_key = '<fixture-uuid-11>'::uuid) AND (timer_id > 'screening'::text))
                ->  Sort  (cost=104.17..104.17 rows=1 width=59)
                      Sort Key: timer_bucket_1.shard_id, timer_bucket_1.fire_at, timer_bucket_1.run_key, timer_bucket_1.timer_id
                      ->  Limit  (cost=100.14..104.16 rows=1 width=59)
                            ->  Index Only Scan using timer_bucket_pkey on timer_bucket timer_bucket_1  (cost=100.14..104.16 rows=1 width=59)
                                  Index Cond: ((shard_id = '<fixture-uuid-10>'::uuid) AND (fire_at = '<fixture-time-4>'::timestamp with time zone) AND (run_key > '<fixture-uuid-11>'::uuid))
                                  -> Storage Scan on timer_bucket_pkey  (cost=100.14..104.16 rows=1 width=59 loops=1)
                                      Projections: shard_id, fire_at, run_key, timer_id
                                      -> B-Tree Scan on timer_bucket_pkey  (cost=100.14..104.16 rows=1 width=59 loops=1)
                                          Index Cond: ((shard_id = '<fixture-uuid-10>'::uuid) AND (fire_at = '<fixture-time-4>'::timestamp with time zone) AND (run_key > '<fixture-uuid-11>'::uuid))
                ->  Sort  (cost=105.70..105.73 rows=11 width=59)
                      Sort Key: timer_bucket_2.shard_id, timer_bucket_2.fire_at, timer_bucket_2.run_key, timer_bucket_2.timer_id
                      ->  Limit  (cost=101.39..105.51 rows=11 width=59)
                            ->  Index Only Scan using timer_bucket_pkey on timer_bucket timer_bucket_2  (cost=101.39..105.51 rows=11 width=59)
                                  Index Cond: ((shard_id = '<fixture-uuid-10>'::uuid) AND (fire_at > '<fixture-time-4>'::timestamp with time zone))
                                  -> Storage Scan on timer_bucket_pkey  (cost=101.39..105.51 rows=11 width=59 loops=1)
                                      Projections: shard_id, fire_at, run_key, timer_id
                                      -> B-Tree Scan on timer_bucket_pkey  (cost=101.39..105.51 rows=11 width=59 loops=1)
                                          Index Cond: ((shard_id = '<fixture-uuid-10>'::uuid) AND (fire_at > '<fixture-time-4>'::timestamp with time zone))
                ->  Limit  (cost=108.52..113.10 rows=68 width=59)
                      ->  Index Only Scan using timer_bucket_pkey on timer_bucket timer_bucket_3  (cost=108.52..113.10 rows=68 width=59)
                            Index Cond: (shard_id > '<fixture-uuid-10>'::uuid)
                            -> Storage Scan on timer_bucket_pkey  (cost=108.52..113.10 rows=68 width=59 loops=1)
                                Projections: shard_id, fire_at, run_key, timer_id
                                -> B-Tree Scan on timer_bucket_pkey  (cost=108.52..113.10 rows=68 width=59 loops=1)
                                    Index Cond: (shard_id > '<fixture-uuid-10>'::uuid)
  InitPlan 2 (returns $1)
    ->  CTE Scan on timer_page  (cost=0.00..1.62 rows=81 width=16)
  ->  Hash Right Join  (cost=103.90..108.03 rows=81 width=145)
        Hash Cond: (workflow_hot.run_key = t.run_key)
        ->  Index Only Scan using workflow_hot_pkey on workflow_hot  (cost=101.26..105.27 rows=9 width=89)
              Index Cond: (run_key = ANY ($1))
              -> Storage Scan on workflow_hot_pkey  (cost=101.26..105.27 rows=10 width=89 loops=1)
                  Projections: run_key, namespace_id, workflow_id
                  -> B-Tree Scan on workflow_hot_pkey  (cost=101.26..105.27 rows=10 width=89 loops=1)
                      Index Cond: (run_key = ANY ($1))
        ->  Hash  (cost=1.62..1.62 rows=81 width=72)
              ->  CTE Scan on timer_page t  (cost=0.00..1.62 rows=81 width=72)

Joined timer continuation offset 11000:
Sort  (cost=525.46..525.50 rows=17 width=145)
  Sort Key: t.shard_id, t.fire_at, t.run_key, t.timer_id
  CTE timer_page
    ->  Limit  (cost=418.82..418.86 rows=17 width=59)
          ->  Sort  (cost=418.82..418.86 rows=17 width=59)
                Sort Key: timer_bucket.shard_id, timer_bucket.fire_at, timer_bucket.run_key, timer_bucket.timer_id
                ->  Append  (cost=100.14..418.47 rows=17 width=59)
                      ->  Limit  (cost=100.14..104.16 rows=1 width=59)
                            ->  Index Only Scan using timer_bucket_pkey on timer_bucket  (cost=100.14..104.16 rows=1 width=59)
                                  Index Cond: ((shard_id = '<fixture-uuid-12>'::uuid) AND (fire_at = '<fixture-time-4>'::timestamp with time zone) AND (run_key = '<fixture-uuid-13>'::uuid) AND (timer_id > 'screening'::text))
                                  -> Storage Scan on timer_bucket_pkey  (cost=100.14..104.16 rows=1 width=59 loops=1)
                                      Projections: shard_id, fire_at, run_key, timer_id
                                      -> B-Tree Scan on timer_bucket_pkey  (cost=100.14..104.16 rows=1 width=59 loops=1)
                                          Index Cond: ((shard_id = '<fixture-uuid-12>'::uuid) AND (fire_at = '<fixture-time-4>'::timestamp with time zone) AND (run_key = '<fixture-uuid-13>'::uuid) AND (timer_id > 'screening'::text))
                      ->  Limit  (cost=100.14..104.16 rows=1 width=59)
                            ->  Index Only Scan using timer_bucket_pkey on timer_bucket timer_bucket_1  (cost=100.14..104.16 rows=1 width=59)
                                  Index Cond: ((shard_id = '<fixture-uuid-12>'::uuid) AND (fire_at = '<fixture-time-4>'::timestamp with time zone) AND (run_key > '<fixture-uuid-13>'::uuid))
                                  -> Storage Scan on timer_bucket_pkey  (cost=100.14..104.16 rows=1 width=59 loops=1)
                                      Projections: shard_id, fire_at, run_key, timer_id
                                      -> B-Tree Scan on timer_bucket_pkey  (cost=100.14..104.16 rows=1 width=59 loops=1)
                                          Index Cond: ((shard_id = '<fixture-uuid-12>'::uuid) AND (fire_at = '<fixture-time-4>'::timestamp with time zone) AND (run_key > '<fixture-uuid-13>'::uuid))
                      ->  Limit  (cost=101.77..105.92 rows=14 width=59)
                            ->  Index Only Scan using timer_bucket_pkey on timer_bucket timer_bucket_2  (cost=101.77..105.92 rows=14 width=59)
                                  Index Cond: ((shard_id = '<fixture-uuid-12>'::uuid) AND (fire_at > '<fixture-time-4>'::timestamp with time zone))
                                  -> Storage Scan on timer_bucket_pkey  (cost=101.77..105.92 rows=14 width=59 loops=1)
                                      Projections: shard_id, fire_at, run_key, timer_id
                                      -> B-Tree Scan on timer_bucket_pkey  (cost=101.77..105.92 rows=14 width=59 loops=1)
                                          Index Cond: ((shard_id = '<fixture-uuid-12>'::uuid) AND (fire_at > '<fixture-time-4>'::timestamp with time zone))
                      ->  Limit  (cost=100.14..104.15 rows=1 width=59)
                            ->  Index Only Scan using timer_bucket_pkey on timer_bucket timer_bucket_3  (cost=100.14..104.15 rows=1 width=59)
                                  Index Cond: (shard_id > '<fixture-uuid-12>'::uuid)
                                  -> Storage Scan on timer_bucket_pkey  (cost=100.14..104.15 rows=1 width=59 loops=1)
                                      Projections: shard_id, fire_at, run_key, timer_id
                                      -> B-Tree Scan on timer_bucket_pkey  (cost=100.14..104.15 rows=1 width=59 loops=1)
                                          Index Cond: (shard_id > '<fixture-uuid-12>'::uuid)
  InitPlan 2 (returns $1)
    ->  CTE Scan on timer_page  (cost=0.00..0.34 rows=17 width=16)
  ->  Hash Right Join  (cost=101.82..105.91 rows=17 width=145)
        Hash Cond: (workflow_hot.run_key = t.run_key)
        ->  Index Only Scan using workflow_hot_pkey on workflow_hot  (cost=101.26..105.27 rows=9 width=89)
              Index Cond: (run_key = ANY ($1))
              -> Storage Scan on workflow_hot_pkey  (cost=101.26..105.27 rows=10 width=89 loops=1)
                  Projections: run_key, namespace_id, workflow_id
                  -> B-Tree Scan on workflow_hot_pkey  (cost=101.26..105.27 rows=10 width=89 loops=1)
                      Index Cond: (run_key = ANY ($1))
        ->  Hash  (cost=0.34..0.34 rows=17 width=72)
              ->  CTE Scan on timer_page t  (cost=0.00..0.34 rows=17 width=72)

Hot continuation:
Limit  (cost=103.64..107.81 rows=29 width=105)
  ->  Index Only Scan using workflow_hot_pkey on workflow_hot  (cost=103.64..107.81 rows=29 width=105)
        -> Storage Scan on workflow_hot_pkey  (cost=103.64..107.81 rows=29 width=105 loops=1)
            Projections: run_key, namespace_id, workflow_id, shard_id
            -> B-Tree Scan on workflow_hot_pkey  (cost=103.64..107.81 rows=29 width=105 loops=1)

Hot continuation:
Limit  (cost=101.39..105.48 rows=11 width=105)
  ->  Index Only Scan using workflow_hot_pkey on workflow_hot  (cost=101.39..105.48 rows=11 width=105)
        Index Cond: (run_key > '<fixture-uuid-14>'::uuid)
        -> Storage Scan on workflow_hot_pkey  (cost=101.39..105.48 rows=11 width=105 loops=1)
            Projections: run_key, namespace_id, workflow_id, shard_id
            -> B-Tree Scan on workflow_hot_pkey  (cost=101.39..105.48 rows=11 width=105 loops=1)
                Index Cond: (run_key > '<fixture-uuid-14>'::uuid)

Committed unchanged rows screened: 24778; elapsed seconds: 4.362; extrapolated seconds for one million runs plus one million timers: 352.1. This extrapolation excludes relocation and is not a million-row benchmark.
```

## Legacy workflow backlog disposal

The physical-key continuation uses the same SQL constant as the running adapter.
The 64-key limit applies before selecting workflow rows, so an activity prefix
cannot require scanning the full prefix within one page. The live regression
also leaves a corrupt workflow key in place and deletes the following covered
key. The fixture UUID is anonymized below.

```sql
SELECT key, run_key, task_kind FROM dispatch_backlog
WHERE key > $1 ORDER BY key LIMIT $2;
```

```text
Limit  (cost=41766.68..41769.78 rows=64 width=34)
  ->  Index Only Scan using dispatch_backlog_pkey on dispatch_backlog  (cost=41766.68..57936.01 rows=333333 width=34)
        Index Cond: (key > '<continuation-key>'::uuid)
        -> Storage Scan on dispatch_backlog_pkey  (cost=41766.68..57936.01 rows=333333 width=34 loops=1)
            Projections: key, task_kind, run_key
            -> B-Tree Scan on dispatch_backlog_pkey  (cost=41766.68..57936.01 rows=333333 width=34 loops=1)
                Index Cond: (key > '<continuation-key>'::uuid)
```
