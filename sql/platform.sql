CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS serving;
CREATE SCHEMA IF NOT EXISTS simulation;

CREATE TABLE IF NOT EXISTS ops.job_runs (
    run_id text NOT NULL, task_id text NOT NULL, attempt integer NOT NULL,
    dataset text NOT NULL, layer text NOT NULL,
    status text NOT NULL CHECK(status IN ('RUNNING','SUCCESS','FAILED')),
    started_at timestamptz NOT NULL, finished_at timestamptz,
    duration_seconds double precision, metrics jsonb NOT NULL DEFAULT '{}',
    error text, log_content text,
    PRIMARY KEY(run_id, task_id, attempt)
);
CREATE INDEX IF NOT EXISTS job_runs_started_idx ON ops.job_runs(started_at DESC);

CREATE TABLE IF NOT EXISTS gold.publications (
    run_id text PRIMARY KEY, published_at timestamptz NOT NULL DEFAULT now(),
    manifest jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS gold.active_publication (
    singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
    run_id text NOT NULL REFERENCES gold.publications(run_id)
);
CREATE TABLE IF NOT EXISTS gold.monthly_cashflow (
    source_run_id text NOT NULL REFERENCES gold.publications(run_id),
    period_month date NOT NULL, transaction_count bigint NOT NULL,
    income_transaction_count bigint NOT NULL, expense_transaction_count bigint NOT NULL,
    total_income numeric(18,2) NOT NULL, total_expenses numeric(18,2) NOT NULL,
    net_cash_flow numeric(18,2) NOT NULL, income_to_expense_ratio numeric(12,6),
    estimated_net_cash_flow_rate_pct numeric(9,4), financial_health_score text NOT NULL,
    PRIMARY KEY(source_run_id, period_month),
    CHECK(total_income >= 0 AND total_expenses >= 0),
    CHECK(net_cash_flow = total_income-total_expenses)
);
CREATE TABLE IF NOT EXISTS gold.category_spending (
    source_run_id text NOT NULL, period_month date NOT NULL, category text NOT NULL,
    expense_transaction_count bigint NOT NULL, expense_total numeric(18,2) NOT NULL,
    expense_share_pct numeric(9,4), PRIMARY KEY(source_run_id,period_month,category),
    FOREIGN KEY(source_run_id,period_month) REFERENCES gold.monthly_cashflow(source_run_id,period_month)
);
CREATE TABLE IF NOT EXISTS gold.credit_cards (
    source_run_id text NOT NULL REFERENCES gold.publications(run_id), cust_id text NOT NULL,
    balance numeric(18,2), credit_limit numeric(18,2), purchases numeric(18,2),
    payments numeric(18,2), credit_utilization_pct numeric(9,4), cluster integer NOT NULL,
    is_anomaly boolean NOT NULL, anomaly_score double precision NOT NULL,
    features jsonb NOT NULL, PRIMARY KEY(source_run_id,cust_id)
);
CREATE TABLE IF NOT EXISTS gold.financial_profiles (
    source_run_id text NOT NULL REFERENCES gold.publications(run_id),
    user_id text NOT NULL, record_date date NOT NULL,
    monthly_income_usd numeric(18,2) NOT NULL, monthly_expenses_usd numeric(18,2) NOT NULL,
    savings_usd numeric(18,2) NOT NULL, credit_score integer NOT NULL,
    savings_capacity numeric(18,2) NOT NULL, emergency_fund_months numeric(12,1) NOT NULL,
    features jsonb NOT NULL, PRIMARY KEY(source_run_id,user_id,record_date)
);

CREATE OR REPLACE VIEW serving.publication AS
SELECT p.* FROM gold.publications p JOIN gold.active_publication a USING(run_id);
CREATE OR REPLACE VIEW serving.monthly_cashflow AS
SELECT m.* FROM gold.monthly_cashflow m JOIN gold.active_publication a ON m.source_run_id=a.run_id;
CREATE OR REPLACE VIEW serving.category_spending AS
SELECT m.* FROM gold.category_spending m JOIN gold.active_publication a ON m.source_run_id=a.run_id;
CREATE OR REPLACE VIEW serving.credit_cards AS
SELECT m.* FROM gold.credit_cards m JOIN gold.active_publication a ON m.source_run_id=a.run_id;
CREATE OR REPLACE VIEW serving.financial_profiles AS
SELECT m.* FROM gold.financial_profiles m JOIN gold.active_publication a ON m.source_run_id=a.run_id;
CREATE OR REPLACE VIEW serving.dim_month AS
SELECT source_run_id, period_month, EXTRACT(year FROM period_month)::int AS year,
       EXTRACT(month FROM period_month)::int AS month FROM serving.monthly_cashflow;
CREATE OR REPLACE VIEW serving.dim_category AS SELECT DISTINCT category FROM serving.category_spending;
CREATE OR REPLACE VIEW serving.dim_card_customer AS SELECT source_run_id,cust_id FROM serving.credit_cards;
CREATE OR REPLACE VIEW serving.dim_profile_user AS SELECT DISTINCT source_run_id,user_id FROM serving.financial_profiles;
CREATE OR REPLACE VIEW serving.card_features AS
SELECT c.source_run_id,c.cust_id,c.credit_utilization_pct,c.cluster,c.is_anomaly,c.anomaly_score,c.features,
       p.published_at AS available_at, 'card-features-v1'::text AS feature_set_version
FROM serving.credit_cards c JOIN gold.publications p ON c.source_run_id=p.run_id;
CREATE OR REPLACE VIEW serving.profile_features AS
SELECT f.source_run_id,f.user_id,f.record_date,f.savings_capacity,f.emergency_fund_months,f.credit_score,f.features,
       p.published_at AS available_at, 'profile-features-v1'::text AS feature_set_version
FROM serving.financial_profiles f JOIN gold.publications p ON f.source_run_id=p.run_id;

CREATE TABLE IF NOT EXISTS simulation.runs (
    simulation_run_id text PRIMARY KEY, source_run_id text NOT NULL REFERENCES gold.publications(run_id),
    created_at timestamptz NOT NULL DEFAULT now(), configuration jsonb NOT NULL,
    historical_months integer NOT NULL CHECK(historical_months >= 3),
    history_hash text NOT NULL, model_version text NOT NULL, output_path text NOT NULL
);
CREATE TABLE IF NOT EXISTS simulation.results (
    simulation_run_id text NOT NULL REFERENCES simulation.runs(simulation_run_id),
    scenario text NOT NULL, month integer NOT NULL,
    period_month date NOT NULL, p05 double precision NOT NULL, p50 double precision NOT NULL,
    p95 double precision NOT NULL, mean_balance double precision NOT NULL,
    probability_negative double precision NOT NULL CHECK(probability_negative BETWEEN 0 AND 1),
    probability_ever_negative double precision NOT NULL CHECK(probability_ever_negative BETWEEN 0 AND 1),
    PRIMARY KEY(simulation_run_id,scenario,month), CHECK(p05 <= p50 AND p50 <= p95)
);

CREATE OR REPLACE VIEW serving.history_publications AS SELECT * FROM gold.publications;
CREATE OR REPLACE VIEW serving.history_monthly_cashflow AS SELECT * FROM gold.monthly_cashflow;
CREATE OR REPLACE VIEW serving.history_category_spending AS SELECT * FROM gold.category_spending;
CREATE OR REPLACE VIEW serving.history_credit_cards AS SELECT * FROM gold.credit_cards;
CREATE OR REPLACE VIEW serving.history_financial_profiles AS SELECT * FROM gold.financial_profiles;
