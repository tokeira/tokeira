INSERT INTO routing_generation (id, generation, updated_at) VALUES (1, 0, now()) ON CONFLICT (id) DO NOTHING;
