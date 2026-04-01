-- Add Serum Capilar Trico Advance (A275)
-- Previously Y938 (promotional code), which was never seeded into the DB.
INSERT INTO medications (sku, brand, manufacturer, product, price)
VALUES ('A275', 'LunaLabs', 'LUNALABS', 'Serum Capilar Trico Advance', NULL)
ON CONFLICT (sku) DO NOTHING;
