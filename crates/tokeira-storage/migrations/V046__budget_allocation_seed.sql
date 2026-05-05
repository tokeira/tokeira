INSERT INTO budget_allocation (id, version, rate_budget, capacity_budget) VALUES (1, 0, 100.0, 10000) ON CONFLICT (id) DO NOTHING;
