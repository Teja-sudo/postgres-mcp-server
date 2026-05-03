/**
 * Complex schema fixture for the audit loop.
 *
 * Builds a realistic, "messy real-world" schema in the target database
 * with the following features so audit subagents can exercise every
 * code path:
 *
 *   - 10+ tables with various column types: integer/bigint, text/varchar
 *     with limits, numeric with precision, boolean, uuid, timestamptz,
 *     date, jsonb, bytea, inet, arrays.
 *   - Primary keys (single + composite)
 *   - Foreign keys (single + composite, with ON DELETE CASCADE/RESTRICT)
 *   - UNIQUE constraints (single + composite)
 *   - CHECK constraints (simple + complex expressions)
 *   - SERIAL / BIGSERIAL columns (sequence-backed)
 *   - GENERATED ALWAYS AS IDENTITY columns
 *   - GENERATED ALWAYS AS (...) STORED columns
 *   - Default values (constants + volatile defaults like now(), gen_random_uuid())
 *   - Enum types
 *   - Composite types
 *   - Views (referencing tables and other views)
 *   - Materialized views
 *   - PL/pgSQL functions (returning scalars, sets, void)
 *   - Procedures
 *   - Triggers (BEFORE INSERT, AFTER UPDATE)
 *   - Btree, GIN, partial, expression indexes
 *   - Comments on every kind
 *
 * Seeds with ~50,000 rows split across the main fact tables.
 */

import { Pool } from 'pg';

export interface ComplexSchemaSeedOptions {
  /** Approximate total row count target (default 50_000). Distributed
   *  across the fact tables proportionally. */
  totalRows?: number;
  /** Skip the heavy seed step and only create schema. Useful for
   *  faster audits that don't care about scale. */
  skipSeed?: boolean;
}

/** Builds the schema in the connected database. Drops public first.  */
export async function buildComplexSchema(
  pool: Pool,
  opts: ComplexSchemaSeedOptions = {}
): Promise<{ tableCount: number; rowCount: number; durationMs: number }> {
  const t0 = Date.now();
  const totalRows = opts.totalRows ?? 50_000;

  await pool.query(`DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO public;`);

  // ------- Extensions -------
  // pgcrypto for gen_random_uuid; available in PG without superuser.
  await pool.query(`CREATE EXTENSION IF NOT EXISTS pgcrypto;`);

  // ------- Enum + composite types -------
  await pool.query(`
    CREATE TYPE order_status AS ENUM ('pending', 'paid', 'shipped', 'cancelled', 'refunded');
    CREATE TYPE address AS (
      street text,
      city text,
      country char(2)
    );
  `);

  // ------- Tables -------
  await pool.query(`
    CREATE TABLE tenants (
      id serial PRIMARY KEY,
      slug text NOT NULL UNIQUE CHECK (slug = lower(slug)),
      name text NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now(),
      metadata jsonb NOT NULL DEFAULT '{}'::jsonb
    );
    COMMENT ON TABLE tenants IS 'Multi-tenant root';
    COMMENT ON COLUMN tenants.slug IS 'URL-safe identifier';

    CREATE TABLE users (
      id bigserial PRIMARY KEY,
      tenant_id int NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
      email text NOT NULL,
      display_name varchar(120),
      country char(2),
      ip_signup inet,
      tags text[] DEFAULT ARRAY[]::text[],
      preferences jsonb DEFAULT '{}'::jsonb,
      is_admin boolean NOT NULL DEFAULT false,
      created_at timestamptz NOT NULL DEFAULT now(),
      uuid uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
      CONSTRAINT users_email_per_tenant UNIQUE (tenant_id, email),
      CONSTRAINT users_email_format CHECK (email LIKE '%@%')
    );
    COMMENT ON TABLE users IS 'Application users';

    CREATE TABLE products (
      id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      sku text NOT NULL,
      name text NOT NULL,
      price numeric(12, 2) NOT NULL CHECK (price >= 0),
      tax_rate numeric(4, 4) NOT NULL DEFAULT 0.0 CHECK (tax_rate BETWEEN 0 AND 1),
      price_with_tax numeric(12, 2) GENERATED ALWAYS AS (round(price * (1 + tax_rate), 2)) STORED,
      categories text[] NOT NULL DEFAULT ARRAY[]::text[],
      attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
      is_active boolean NOT NULL DEFAULT true,
      CONSTRAINT products_sku_unique UNIQUE (sku)
    );

    CREATE TABLE orders (
      id bigserial,
      tenant_id int NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
      user_id bigint NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
      status order_status NOT NULL DEFAULT 'pending',
      total numeric(14, 2) NOT NULL DEFAULT 0,
      currency char(3) NOT NULL DEFAULT 'USD',
      shipping_address address,
      placed_at timestamptz NOT NULL DEFAULT now(),
      raw_payload jsonb,
      audit_blob bytea,
      PRIMARY KEY (id, tenant_id),
      CONSTRAINT orders_currency_iso CHECK (currency ~ '^[A-Z]{3}$')
    );

    CREATE TABLE order_items (
      order_id bigint NOT NULL,
      tenant_id int NOT NULL,
      product_id int NOT NULL REFERENCES products(id) ON DELETE RESTRICT,
      quantity int NOT NULL CHECK (quantity > 0),
      unit_price numeric(12, 2) NOT NULL,
      line_total numeric(14, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
      PRIMARY KEY (order_id, tenant_id, product_id),
      FOREIGN KEY (order_id, tenant_id) REFERENCES orders(id, tenant_id) ON DELETE CASCADE
    );

    CREATE TABLE audit_log (
      id bigserial PRIMARY KEY,
      tenant_id int,
      user_id bigint,
      action text NOT NULL,
      entity_type text NOT NULL,
      entity_id text NOT NULL,
      details jsonb DEFAULT '{}'::jsonb,
      occurred_at timestamptz NOT NULL DEFAULT clock_timestamp()
    );
  `);

  // ------- Indexes (btree + partial + expression + GIN) -------
  await pool.query(`
    CREATE INDEX idx_users_tenant ON users (tenant_id);
    CREATE INDEX idx_users_country_active ON users (country) WHERE is_admin = false;
    CREATE INDEX idx_users_lower_name ON users (lower(display_name));
    CREATE INDEX idx_users_tags_gin ON users USING gin (tags);
    CREATE INDEX idx_users_prefs_gin ON users USING gin (preferences);

    CREATE INDEX idx_orders_tenant_user ON orders (tenant_id, user_id);
    CREATE INDEX idx_orders_status ON orders (status) WHERE status IN ('pending', 'paid');
    CREATE INDEX idx_orders_placed_at_desc ON orders (placed_at DESC);

    CREATE INDEX idx_audit_entity ON audit_log (entity_type, entity_id);
    CREATE INDEX idx_audit_occurred_brin ON audit_log USING brin (occurred_at);

    CREATE INDEX idx_products_categories_gin ON products USING gin (categories);
  `);

  // ------- Views -------
  await pool.query(`
    CREATE VIEW active_users AS
      SELECT u.id, u.tenant_id, lower(u.email) AS email, u.country, u.created_at
      FROM users u
      WHERE u.is_admin = false;
    COMMENT ON VIEW active_users IS 'Non-admin users';

    CREATE VIEW order_summary AS
      SELECT
        o.id, o.tenant_id, o.user_id, o.status, o.total, o.currency,
        lower(u.email) AS user_email,
        t.slug AS tenant_slug
      FROM orders o
      JOIN users u ON u.id = o.user_id
      JOIN tenants t ON t.id = o.tenant_id;

    CREATE MATERIALIZED VIEW tenant_revenue AS
      SELECT
        t.id AS tenant_id, t.slug, count(o.id) AS order_count,
        coalesce(sum(o.total) FILTER (WHERE o.status IN ('paid','shipped')), 0) AS revenue
      FROM tenants t
      LEFT JOIN orders o ON o.tenant_id = t.id
      GROUP BY t.id, t.slug;
    CREATE UNIQUE INDEX tenant_revenue_pk ON tenant_revenue (tenant_id);
  `);

  // ------- Functions + Procedures -------
  await pool.query(`
    CREATE OR REPLACE FUNCTION recalc_order_total(p_order_id bigint, p_tenant_id int)
    RETURNS numeric LANGUAGE plpgsql AS $$
    DECLARE
      total_amount numeric;
    BEGIN
      SELECT coalesce(sum(line_total), 0) INTO total_amount
      FROM order_items
      WHERE order_id = p_order_id AND tenant_id = p_tenant_id;
      UPDATE orders SET total = total_amount
      WHERE id = p_order_id AND tenant_id = p_tenant_id;
      RETURN total_amount;
    END $$;

    CREATE OR REPLACE FUNCTION top_users_by_orders(p_limit int DEFAULT 10)
    RETURNS TABLE (user_id bigint, email text, order_count bigint)
    LANGUAGE sql STABLE AS $$
      SELECT u.id, lower(u.email), count(o.id)
      FROM users u
      LEFT JOIN orders o ON o.user_id = u.id
      GROUP BY u.id, u.email
      ORDER BY count(o.id) DESC
      LIMIT p_limit;
    $$;

    CREATE OR REPLACE PROCEDURE archive_old_audit(older_than interval)
    LANGUAGE plpgsql AS $$
    BEGIN
      DELETE FROM audit_log WHERE occurred_at < now() - older_than;
    END $$;
  `);

  // ------- Triggers -------
  await pool.query(`
    CREATE OR REPLACE FUNCTION audit_user_change()
    RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      INSERT INTO audit_log (tenant_id, user_id, action, entity_type, entity_id, details)
      VALUES (
        NEW.tenant_id, NEW.id,
        TG_OP, 'user', NEW.id::text,
        jsonb_build_object('email', lower(NEW.email))
      );
      RETURN NEW;
    END $$;

    CREATE TRIGGER trg_audit_users_after_insert
      AFTER INSERT ON users
      FOR EACH ROW EXECUTE FUNCTION audit_user_change();

    CREATE OR REPLACE FUNCTION enforce_tenant_consistency()
    RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF EXISTS (SELECT 1 FROM users WHERE id = NEW.user_id AND tenant_id <> NEW.tenant_id) THEN
        RAISE EXCEPTION 'order tenant_id % does not match user tenant_id', NEW.tenant_id;
      END IF;
      RETURN NEW;
    END $$;

    CREATE TRIGGER trg_orders_tenant_consistent
      BEFORE INSERT OR UPDATE ON orders
      FOR EACH ROW EXECUTE FUNCTION enforce_tenant_consistency();
  `);

  // ------- Seed data -------
  let rowCount = 0;
  if (!opts.skipSeed) {
    // Distribute totalRows: ~5% tenants, ~30% users, ~50% orders, ~15% audit
    const nTenants = Math.max(5, Math.round(totalRows * 0.001));
    const nUsersPerTenant = Math.max(20, Math.round((totalRows * 0.3) / nTenants));
    const nOrdersPerUser = Math.max(2, Math.round((totalRows * 0.5) / (nTenants * nUsersPerTenant)));

    // Tenants
    await pool.query(
      `INSERT INTO tenants (slug, name)
       SELECT 'tenant_' || g, 'Tenant ' || g
       FROM generate_series(1, $1) g`,
      [nTenants]
    );

    // Products (constant 50)
    await pool.query(`
      INSERT INTO products (sku, name, price, tax_rate, categories)
      SELECT
        'sku-' || lpad(g::text, 5, '0'),
        'Product ' || g,
        round((random() * 999 + 1)::numeric, 2),
        (ARRAY[0.0, 0.05, 0.10, 0.15, 0.20])[1 + (g % 5)],
        ARRAY[
          (ARRAY['electronics', 'books', 'clothing', 'food', 'home'])[1 + (g % 5)],
          (ARRAY['new', 'sale', 'featured'])[1 + (g % 3)]
        ]
      FROM generate_series(1, 50) g
    `);

    // Users (cast params to int to disambiguate operators)
    await pool.query(
      `INSERT INTO users (tenant_id, email, display_name, country, is_admin, ip_signup, tags, preferences)
       SELECT
         ((g - 1) % $1::int) + 1,
         'user' || g || '@example.com',
         'User ' || g,
         (ARRAY['US', 'GB', 'DE', 'FR', 'JP', 'IN'])[1 + (g % 6)],
         (g % 100 = 0),
         ('10.' || ((g >> 16) % 256) || '.' || ((g >> 8) % 256) || '.' || (g % 256))::inet,
         ARRAY[
           (ARRAY['vip', 'beta', 'newsletter'])[1 + (g % 3)],
           'cohort_' || (g % 10)
         ],
         jsonb_build_object('theme', (ARRAY['light', 'dark'])[1 + (g % 2)], 'lang', 'en')
       FROM generate_series(1, ($1::int * $2::int)) g`,
      [nTenants, nUsersPerTenant]
    );

    // Orders + items
    await pool.query(
      `WITH placed AS (
         INSERT INTO orders (tenant_id, user_id, status, total, currency, shipping_address, raw_payload)
         SELECT
           u.tenant_id, u.id,
           (ARRAY['pending', 'paid', 'shipped', 'cancelled']::order_status[])[1 + (g % 4)],
           0,
           (ARRAY['USD', 'EUR', 'GBP', 'JPY'])[1 + (g % 4)],
           ROW('123 Test St', 'Springfield', 'US')::address,
           jsonb_build_object('source', 'audit_seed', 'iteration', g)
         FROM users u, generate_series(1, $1::int) g
         RETURNING id, tenant_id
       )
       INSERT INTO order_items (order_id, tenant_id, product_id, quantity, unit_price)
       SELECT
         o.id, o.tenant_id,
         ((random() * 49)::int) + 1,
         1 + (random() * 4)::int,
         round((random() * 100 + 1)::numeric, 2)
       FROM placed o`,
      [nOrdersPerUser]
    );

    // Audit log naturally populated by trigger; add some manual rows too
    await pool.query(`
      INSERT INTO audit_log (tenant_id, user_id, action, entity_type, entity_id, details, occurred_at)
      SELECT
        u.tenant_id, u.id,
        (ARRAY['LOGIN', 'LOGOUT', 'UPDATE'])[1 + (g % 3)],
        (ARRAY['session', 'profile', 'order'])[1 + (g % 3)],
        u.id::text,
        jsonb_build_object('seed', g),
        now() - (g || ' minutes')::interval
      FROM users u, generate_series(1, 3) g
    `);

    // Refresh matview now that data exists
    await pool.query(`REFRESH MATERIALIZED VIEW tenant_revenue;`);

    const counts = await pool.query(`
      SELECT
        (SELECT count(*) FROM tenants) +
        (SELECT count(*) FROM users) +
        (SELECT count(*) FROM products) +
        (SELECT count(*) FROM orders) +
        (SELECT count(*) FROM order_items) +
        (SELECT count(*) FROM audit_log) AS total
    `);
    rowCount = Number(counts.rows[0].total);
  }

  // Stats so EXPLAIN-related tools see realistic plans
  await pool.query(`ANALYZE;`);

  const tableCountR = await pool.query(`
    SELECT count(*)::int AS c FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relkind = 'r'
  `);

  return {
    tableCount: tableCountR.rows[0].c,
    rowCount,
    durationMs: Date.now() - t0,
  };
}
