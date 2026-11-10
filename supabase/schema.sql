-- Task Portal database schema (Supabase / PostgreSQL).
-- Already applied to the live project. Keep this file as the record, or run it
-- in a fresh Supabase project's SQL editor to rebuild the database.

-- ========== PROFILES & ROLES ==========
create table public.profiles (
  id uuid primary key references auth.users(id) on delete cascade,
  full_name text not null default '',
  email text,
  role text not null default 'pending' check (role in ('admin','engineer','manager','pending')),
  created_at timestamptz not null default now()
);

create or replace function public.my_role() returns text
language sql stable security definer set search_path = public as $$
  select role from public.profiles where id = auth.uid()
$$;

create or replace function public.is_active() returns boolean
language sql stable security definer set search_path = public as $$
  select coalesce((select role in ('admin','engineer','manager') from public.profiles where id = auth.uid()), false)
$$;

-- Every new sign-up gets a profile with role 'pending' until an admin approves it
create or replace function public.handle_new_user() returns trigger
language plpgsql security definer set search_path = public as $$
begin
  insert into public.profiles (id, full_name, email, role)
  values (new.id, coalesce(new.raw_user_meta_data->>'full_name', split_part(new.email, '@', 1)), new.email, 'pending')
  on conflict (id) do nothing;
  return new;
end $$;
create trigger on_auth_user_created after insert on auth.users
  for each row execute function public.handle_new_user();

create or replace function public.set_user_role(target uuid, new_role text) returns void
language plpgsql security definer set search_path = public as $$
begin
  if public.my_role() <> 'admin' then raise exception 'Only an admin can change roles'; end if;
  if new_role not in ('admin','engineer','manager','pending') then raise exception 'Invalid role'; end if;
  if target = auth.uid() and new_role <> 'admin' then raise exception 'You cannot remove your own admin role'; end if;
  update public.profiles set role = new_role where id = target;
end $$;

alter table public.profiles enable row level security;
create policy "profiles readable by active users or self" on public.profiles
  for select to authenticated using (public.is_active() or id = auth.uid());
create policy "update own profile" on public.profiles
  for update to authenticated using (id = auth.uid()) with check (id = auth.uid());
revoke update on public.profiles from authenticated, anon;
grant update (full_name) on public.profiles to authenticated;

-- ========== TICKETS ==========
create table public.tickets (
  id bigint generated always as identity primary key,
  key text generated always as ('TSK-' || lpad(id::text, 4, '0')) stored,
  title text not null check (length(trim(title)) > 0),
  description text not null default '',
  type text not null default 'admin' check (type in
    ('cdc_onboarding','kafka_stream','informatica','incident','data_quality','report','access','documentation','learning','enhancement','admin')),
  priority text not null default 'P3' check (priority in ('P1','P2','P3','P4')),
  status text not null default 'new' check (status in ('new','in_progress','blocked','in_review','done','cancelled')),
  system text not null default 'Other' check (system in
    ('Informatica','Kafka','PostgreSQL','Oracle','Cloudera','Greenplum','MicroStrategy','Linux / Servers','Other')),
  layer text not null default 'N/A' check (layer in ('Source','Raw','Enrich','Staging','Core','Semantic','Reporting','N/A')),
  reporter_id uuid references public.profiles(id) default auth.uid(),
  assignee_id uuid references public.profiles(id),
  due_date date,
  estimate_hours numeric(6,2) check (estimate_hours is null or estimate_hours >= 0),
  blocked_reason text,
  approved_by uuid references public.profiles(id),
  approved_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  started_at timestamptz,
  resolved_at timestamptz
);
create index on public.tickets (status);
create index on public.tickets (assignee_id);
create index on public.tickets (updated_at desc);

create table public.ticket_comments (
  id bigint generated always as identity primary key,
  ticket_id bigint not null references public.tickets(id) on delete cascade,
  author_id uuid not null references public.profiles(id) default auth.uid(),
  body text not null check (length(trim(body)) > 0),
  created_at timestamptz not null default now()
);
create index on public.ticket_comments (ticket_id);

create table public.work_logs (
  id bigint generated always as identity primary key,
  ticket_id bigint not null references public.tickets(id) on delete cascade,
  user_id uuid not null references public.profiles(id) default auth.uid(),
  work_date date not null default current_date,
  hours numeric(5,2) not null check (hours > 0 and hours <= 24),
  note text not null default '',
  created_at timestamptz not null default now()
);
create index on public.work_logs (ticket_id);
create index on public.work_logs (work_date);

create table public.ticket_events (
  id bigint generated always as identity primary key,
  ticket_id bigint not null references public.tickets(id) on delete cascade,
  actor_id uuid references public.profiles(id),
  event text not null,
  from_value text,
  to_value text,
  created_at timestamptz not null default now()
);
create index on public.ticket_events (ticket_id);
create index on public.ticket_events (created_at desc);

-- ========== LIFECYCLE TRIGGERS ==========
create or replace function public.tickets_before() returns trigger
language plpgsql set search_path = public as $$
begin
  if tg_op = 'UPDATE' then
    new.updated_at := now();
    if new.status = 'in_progress' and new.started_at is null then new.started_at := now(); end if;
    if new.status = 'done' and old.status <> 'done' then new.resolved_at := now(); end if;
    if new.status <> 'done' and old.status = 'done' then
      new.resolved_at := null; new.approved_by := null; new.approved_at := null;
    end if;
    if new.status <> 'blocked' then new.blocked_reason := null; end if;
  else
    if new.status = 'in_progress' then new.started_at := now(); end if;
    if new.status = 'done' then new.resolved_at := now(); end if;
  end if;
  return new;
end $$;
create trigger tickets_before before insert or update on public.tickets
  for each row execute function public.tickets_before();

-- Automatic history: created, status, assignee, priority, due date, sign-off
create or replace function public.tickets_after() returns trigger
language plpgsql security definer set search_path = public as $$
declare a uuid := auth.uid();
begin
  if tg_op = 'INSERT' then
    insert into ticket_events (ticket_id, actor_id, event, to_value) values (new.id, a, 'created', new.status);
    return new;
  end if;
  if new.status is distinct from old.status then
    insert into ticket_events (ticket_id, actor_id, event, from_value, to_value) values (new.id, a, 'status', old.status, new.status);
  end if;
  if new.assignee_id is distinct from old.assignee_id then
    insert into ticket_events (ticket_id, actor_id, event, from_value, to_value)
    values (new.id, a, 'assignee',
      (select full_name from profiles where id = old.assignee_id), (select full_name from profiles where id = new.assignee_id));
  end if;
  if new.priority is distinct from old.priority then
    insert into ticket_events (ticket_id, actor_id, event, from_value, to_value) values (new.id, a, 'priority', old.priority, new.priority);
  end if;
  if new.due_date is distinct from old.due_date then
    insert into ticket_events (ticket_id, actor_id, event, from_value, to_value) values (new.id, a, 'due_date', old.due_date::text, new.due_date::text);
  end if;
  if new.approved_at is not null and old.approved_at is null then
    insert into ticket_events (ticket_id, actor_id, event) values (new.id, a, 'signed_off');
  end if;
  return new;
end $$;
create trigger tickets_after after insert or update on public.tickets
  for each row execute function public.tickets_after();

-- Only a manager can sign off, and only Done tickets
create or replace function public.sign_off(t bigint) returns void
language plpgsql security definer set search_path = public as $$
begin
  if public.my_role() <> 'manager' then raise exception 'Only a manager can sign off work'; end if;
  update tickets set approved_by = auth.uid(), approved_at = now() where id = t and status = 'done';
  if not found then raise exception 'Ticket must be Done before sign-off'; end if;
end $$;

-- ========== ROW LEVEL SECURITY ==========
alter table public.tickets enable row level security;
alter table public.ticket_comments enable row level security;
alter table public.work_logs enable row level security;
alter table public.ticket_events enable row level security;

create policy "read tickets" on public.tickets for select to authenticated using (public.is_active());
create policy "create tickets" on public.tickets for insert to authenticated
  with check (public.is_active() and reporter_id = auth.uid());
create policy "update tickets" on public.tickets for update to authenticated
  using (public.is_active()) with check (public.is_active());
create policy "admin deletes tickets" on public.tickets for delete to authenticated using (public.my_role() = 'admin');

create policy "read comments" on public.ticket_comments for select to authenticated using (public.is_active());
create policy "add comments" on public.ticket_comments for insert to authenticated
  with check (public.is_active() and author_id = auth.uid());
create policy "delete own comments" on public.ticket_comments for delete to authenticated using (author_id = auth.uid());

create policy "read work logs" on public.work_logs for select to authenticated using (public.is_active());
create policy "log own work" on public.work_logs for insert to authenticated
  with check (public.my_role() in ('engineer','admin') and user_id = auth.uid());
create policy "edit own work" on public.work_logs for update to authenticated
  using (user_id = auth.uid()) with check (user_id = auth.uid());
create policy "delete own work" on public.work_logs for delete to authenticated using (user_id = auth.uid());

create policy "read events" on public.ticket_events for select to authenticated using (public.is_active());

-- Sign-off columns change only through sign_off()
revoke update on public.tickets from authenticated, anon;
grant update (title, description, type, priority, status, system, layer, assignee_id, due_date, estimate_hours, blocked_reason)
  on public.tickets to authenticated;
revoke insert on public.tickets from anon;
revoke all on public.ticket_comments, public.work_logs, public.ticket_events from anon;
revoke insert, update, delete on public.ticket_events from authenticated;
revoke all on function public.set_user_role(uuid, text) from anon;
revoke all on function public.sign_off(bigint) from anon;

-- Function access: trigger functions are not callable; helpers and actions need a signed-in user
revoke execute on function public.handle_new_user() from public, anon, authenticated;
revoke execute on function public.tickets_after() from public, anon, authenticated;
revoke execute on function public.my_role() from public, anon;
revoke execute on function public.is_active() from public, anon;
revoke execute on function public.set_user_role(uuid, text) from public, anon;
revoke execute on function public.sign_off(bigint) from public, anon;
grant execute on function public.my_role() to authenticated;
grant execute on function public.is_active() to authenticated;
grant execute on function public.set_user_role(uuid, text) to authenticated;
grant execute on function public.sign_off(bigint) to authenticated;
