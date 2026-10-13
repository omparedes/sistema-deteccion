-- MVP académico. Esquema inicial; no hay políticas de acceso público.
-- Ejecutar una vez en PostgreSQL/Supabase de desarrollo, previa revisión.
create extension if not exists pgcrypto;

create table if not exists public.data_sources (
  id uuid primary key default gen_random_uuid(),
  source_code text not null unique,
  description text,
  source_format text not null,
  is_synthetic boolean not null default true,
  created_at timestamptz not null default now()
);

create table if not exists public.actors (
  id uuid primary key default gen_random_uuid(),
  actor_code text not null unique,
  role_name text,
  department text,
  is_synthetic boolean not null default true,
  created_at timestamptz not null default now()
);

create table if not exists public.raw_events (
  id uuid primary key default gen_random_uuid(),
  source_id uuid not null references public.data_sources(id),
  external_event_id text,
  received_at timestamptz not null default now(),
  payload jsonb not null,
  processing_status text not null default 'pending'
    check (processing_status in ('pending', 'processed', 'rejected', 'duplicate')),
  processing_error text,
  is_synthetic boolean not null default true
);
create index if not exists idx_raw_events_source on public.raw_events(source_id, received_at desc);

create table if not exists public.normalized_events (
  id uuid primary key default gen_random_uuid(),
  raw_event_id uuid not null unique references public.raw_events(id),
  actor_id uuid references public.actors(id),
  occurred_at timestamptz not null,
  operation text not null,
  device_code text,
  session_code text,
  file_extension text,
  file_type text,
  file_size_bytes bigint not null check (file_size_bytes >= 0),
  source_type text,
  source_path text,
  destination_type text,
  destination_path text,
  destination_authorized boolean,
  data_classification text,
  operation_result text,
  is_synthetic boolean not null default true,
  normalized_at timestamptz not null default now()
);
create index if not exists idx_norm_events_actor_time on public.normalized_events(actor_id, occurred_at desc);

create table if not exists public.feature_windows (
  id uuid primary key default gen_random_uuid(),
  actor_id uuid not null references public.actors(id),
  window_start timestamptz not null,
  window_end timestamptz not null,
  features jsonb not null,
  pipeline_version text not null,
  is_synthetic boolean not null default true,
  created_at timestamptz not null default now(),
  constraint window_valid check (window_end > window_start)
);
create index if not exists idx_feature_windows_actor_time on public.feature_windows(actor_id, window_start desc);

create table if not exists public.model_versions (
  id uuid primary key default gen_random_uuid(),
  model_name text not null,
  model_version text not null,
  parameters jsonb not null default '{}'::jsonb,
  feature_schema_version text not null,
  created_at timestamptz not null default now(),
  unique(model_name, model_version)
);

create table if not exists public.detection_results (
  id uuid primary key default gen_random_uuid(),
  feature_window_id uuid not null references public.feature_windows(id),
  model_version_id uuid references public.model_versions(id),
  anomaly_score double precision,
  rule_score double precision,
  risk_score double precision,
  rule_hits jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now()
);
create index if not exists idx_detection_window on public.detection_results(feature_window_id);

create table if not exists public.alerts (
  id uuid primary key default gen_random_uuid(),
  detection_result_id uuid not null references public.detection_results(id),
  severity text not null check (severity in ('low', 'medium', 'high')),
  status text not null default 'open' check (status in ('open', 'reviewing', 'closed')),
  explanation text,
  created_at timestamptz not null default now()
);

create table if not exists public.alert_reviews (
  id uuid primary key default gen_random_uuid(),
  alert_id uuid not null references public.alerts(id),
  analyst_reference text not null,
  classification text not null check (classification in ('expected', 'investigate', 'confirmed_in_simulation')),
  notes text,
  created_at timestamptz not null default now()
);

-- RLS preventivo: sin políticas, no hay accesos directos para anon/authenticated.
-- Crear permisos y políticas de mínimos privilegios cuando la API/auth estén listos.
alter table public.data_sources enable row level security;
alter table public.actors enable row level security;
alter table public.raw_events enable row level security;
alter table public.normalized_events enable row level security;
alter table public.feature_windows enable row level security;
alter table public.model_versions enable row level security;
alter table public.detection_results enable row level security;
alter table public.alerts enable row level security;
alter table public.alert_reviews enable row level security;
