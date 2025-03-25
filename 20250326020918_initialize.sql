create or replace function create_audit_table(table_name text)
  returns void
  volatile
  security definer
  language plpgsql
as $$
declare
  audit_table text = concat($1, '_audit');
  audit_function text = concat($1, '_record_change');

  table_sql text = format(E'
    create table if not exists %I (
      action text not null,
      action_time timestamptz not null default current_timestamp,
      session_username text not null default session_user::text,
      application_name text default current_setting(\'application_name\', true),
      application_user_id text default current_setting(\'app.user_id\', true),
      application_username text default current_setting(\'app.username\', true),
      old_data jsonb,
      new_data jsonb
    )',
    audit_table
  );

  audit_sql text = format(E'
    create or replace function %I()
      returns trigger
      volatile
      security definer
      language plpgsql
    as $body$
    begin
      if (TG_OP = \'DELETE\') then
        insert into %2$I (action, old_data)
          values (TG_OP, row_to_json(OLD)::JSONB);
        return old;
      elsif (TG_OP = \'INSERT\') then
        insert into %2$I (action, new_data)
          values (TG_OP, row_to_json(NEW)::JSONB);
          return new;
      elsif (TG_OP = \'UPDATE\') then
        insert into %2$I (action, old_data, new_data)
          values (TG_OP, row_to_json(OLD)::JSONB, row_to_json(NEW)::JSONB);
        return new;
      end if;
    end
    $body$',
    audit_function, audit_table);

  trigger_sql text = format('
    create or replace trigger %I_record_change
      after insert or update or delete
      on %1$I
      for each row
      execute procedure %2$I()
    ',
    $1, audit_function);
begin
  execute table_sql;
  execute audit_sql;
  execute trigger_sql;
end;
$$;

CREATE TABLE IF NOT EXISTS kubernetes_objects
(
    api_version TEXT NOT NULL,
    cluster TEXT NOT NULL,
    created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    kind TEXT NOT NULL,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    name TEXT NOT NULL,
    -- set un-namespaced objects to namespace "" to avoid composite primary key issues
    namespace TEXT NOT NULL,
    yaml TEXT NOT NULL,
    PRIMARY KEY (name, namespace, kind, api_version, cluster)
);

SELECT create_audit_table('kubernetes_objects');
