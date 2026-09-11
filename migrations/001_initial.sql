CREATE TABLE users(id uuid PRIMARY KEY,email text UNIQUE NOT NULL,password_hash text NOT NULL,
    role text NOT NULL DEFAULT 'user' CHECK(role IN ('user','admin')),
    budget bigint NOT NULL DEFAULT 1000000 CHECK(budget>=0),spent bigint NOT NULL DEFAULT 0 CHECK(spent>=0),
    reserved bigint NOT NULL DEFAULT 0 CHECK(reserved>=0));
CREATE TABLE generations(id uuid PRIMARY KEY,user_id uuid NOT NULL REFERENCES users(id),request_key text NOT NULL,
    body_hash text NOT NULL,status text NOT NULL DEFAULT 'running' CHECK(status IN ('running','completed','failed','cancelled','abandoned')),
    reserve bigint NOT NULL CHECK(reserve>=0),charge bigint NOT NULL DEFAULT 0,lease_until timestamptz NOT NULL,
    result jsonb,error text,created_at timestamptz NOT NULL DEFAULT now(),finished_at timestamptz,
    UNIQUE(user_id,request_key));
CREATE INDEX generations_expiry ON generations(status,lease_until);
CREATE TABLE attempts(id uuid PRIMARY KEY,generation_id uuid NOT NULL REFERENCES generations(id),provider text NOT NULL,
    price_version text NOT NULL DEFAULT 'demo-v1',input_price integer NOT NULL,output_price integer NOT NULL,
    cap bigint NOT NULL,charge bigint NOT NULL DEFAULT 0,status text NOT NULL DEFAULT 'running',
    accounting text NOT NULL DEFAULT 'reserved',input_tokens integer,output_tokens integer,error text,
    started_at timestamptz NOT NULL DEFAULT now(),finished_at timestamptz);
CREATE TABLE rate_windows(user_id uuid REFERENCES users(id),minute timestamptz NOT NULL,requests integer NOT NULL,
    PRIMARY KEY(user_id,minute));
CREATE TABLE circuits(provider text PRIMARY KEY,failures integer NOT NULL DEFAULT 0,epoch bigint NOT NULL DEFAULT 0,
    open_until timestamptz,probe_until timestamptz);
