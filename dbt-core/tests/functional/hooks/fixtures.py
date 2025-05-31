macros__before_and_after = """
{% macro custom_run_hook(state, target, run_started_at, invocation_id) %}

   insert into {{ target.schema }}.on_run_hook (
        "state",
        "target.dbname",
        "target.host",
        "target.name",
        "target.schema",
        "target.type",
        "target.user",
        "target.pass",
        "target.port",
        "target.threads",
        "run_started_at",
        "invocation_id"
   ) VALUES (
    '{{ state }}',
    '{{ target.dbname }}',
    '{{ target.host }}',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    '{{ target.user }}',
    '{{ target.get("pass", "") }}',
    {{ target.port }},
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}'
   )

{% endmacro %}
"""

macros__hook = """
{% macro hook() %}
  select 1
{% endmacro %}
"""

models__hooks = """
select 1 as id
"""

models__hooks_configured = """
{{
    config({
        "pre_hook": "\
            insert into {{this.schema}}.on_model_hook (\
                \\"state\\",\
                \\"target.dbname\\",\
                \\"target.host\\",\
                \\"target.name\\",\
                \\"target.schema\\",\
                \\"target.type\\",\
                \\"target.user\\",\
                \\"target.pass\\",\
                \\"target.port\\",\
                \\"target.threads\\",\
                \\"run_started_at\\",\
                \\"invocation_id\\"\
            ) VALUES (\
                'start',\
                '{{ target.dbname }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.port }},\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}'\
        )",
        "post-hook": "\
            insert into {{this.schema}}.on_model_hook (\
                \\"state\\",\
                \\"target.dbname\\",\
                \\"target.host\\",\
                \\"target.name\\",\
                \\"target.schema\\",\
                \\"target.type\\",\
                \\"target.user\\",\
                \\"target.pass\\",\
                \\"target.port\\",\
                \\"target.threads\\",\
                \\"run_started_at\\",\
                \\"invocation_id\\"\
            ) VALUES (\
                'end',\
                '{{ target.dbname }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.port }},\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}'\
            )"
    })
}}

select 3 as id
"""

models__hooks_error = """
{{
    config({
        "pre_hook": "\
            insert into {{this.schema}}.on_model_hook (\
                \\"state\\",\
                \\"target.dbname\\",\
                \\"target.host\\",\
                \\"target.name\\",\
                \\"target.schema\\",\
                \\"target.type\\",\
                \\"target.user\\",\
                \\"target.pass\\",\
                \\"target.port\\",\
                \\"target.threads\\",\
                \\"run_started_at\\",\
                \\"invocation_id\\"\
            ) VALUES (\
                'start',\
                '{{ target.dbname }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.port }},\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}'\
        )",
        "pre-hook": "\
            insert into {{this.schema}}.on_model_hook (\
                \\"state\\",\
                \\"target.dbname\\",\
                \\"target.host\\",\
                \\"target.name\\",\
                \\"target.schema\\",\
                \\"target.type\\",\
                \\"target.user\\",\
                \\"target.pass\\",\
                \\"target.port\\",\
                \\"target.threads\\",\
                \\"run_started_at\\",\
                \\"invocation_id\\"\
            ) VALUES (\
                'start',\
                '{{ target.dbname }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.port }},\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}'\
        )",
        "post-hook": "\
            insert into {{this.schema}}.on_model_hook (\
                \\"state\\",\
                \\"target.dbname\\",\
                \\"target.host\\",\
                \\"target.name\\",\
                \\"target.schema\\",\
                \\"target.type\\",\
                \\"target.user\\",\
                \\"target.pass\\",\
                \\"target.port\\",\
                \\"target.threads\\",\
                \\"run_started_at\\",\
                \\"invocation_id\\"\
            ) VALUES (\
                'end',\
                '{{ target.dbname }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.port }},\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}'\
            )"
    })
}}

select 3 as id
"""

models__hooks_kwargs = """
{{
    config(
        pre_hook="\
            insert into {{this.schema}}.on_model_hook (\
                \\"state\\",\
                \\"target.dbname\\",\
                \\"target.host\\",\
                \\"target.name\\",\
                \\"target.schema\\",\
                \\"target.type\\",\
                \\"target.user\\",\
                \\"target.pass\\",\
                \\"target.port\\",\
                \\"target.threads\\",\
                \\"run_started_at\\",\
                \\"invocation_id\\"\
            ) VALUES (\
                'start',\
                '{{ target.dbname }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.port }},\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}'\
        )",
        post_hook="\
            insert into {{this.schema}}.on_model_hook (\
                \\"state\\",\
                \\"target.dbname\\",\
                \\"target.host\\",\
                \\"target.name\\",\
                \\"target.schema\\",\
                \\"target.type\\",\
                \\"target.user\\",\
                \\"target.pass\\",\
                \\"target.port\\",\
                \\"target.threads\\",\
                \\"run_started_at\\",\
                \\"invocation_id\\"\
            ) VALUES (\
                'end',\
                '{{ target.dbname }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.port }},\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}'\
            )"
    )
}}

select 3 as id
"""

models__hooked = """
{{
    config({
        "pre_hook": "\
            insert into {{this.schema}}.on_model_hook select
                state,
                '{{ target.dbname }}' as \\"target.dbname\\",\
                '{{ target.host }}' as \\"target.host\\",\
                '{{ target.name }}' as \\"target.name\\",\
                '{{ target.schema }}' as \\"target.schema\\",\
                '{{ target.type }}' as \\"target.type\\",\
                '{{ target.user }}' as \\"target.user\\",\
                '{{ target.get(\\"pass\\", \\"\\") }}' as \\"target.pass\\",\
                {{ target.port }} as \\"target.port\\",\
                {{ target.threads }} as \\"target.threads\\",\
                '{{ run_started_at }}' as \\"run_started_at\\",\
                '{{ invocation_id }}' as \\"invocation_id\\"\
                from {{ ref('pre') }}\
        "
    })
}}
select 1 as id
"""

models__post = """
select 'end' as state
"""

models__pre = """
select 'start' as state
"""

snapshots__test_snapshot = """
{% snapshot example_snapshot %}
{{
    config(target_schema=schema, unique_key='a', strategy='check', check_cols='all')
}}
select * from {{ ref('example_seed') }}
{% endsnapshot %}
"""

properties__seed_models = """
version: 2
seeds:
- name: example_seed
  columns:
  - name: new_col
    tests:
    - not_null
"""

properties__test_snapshot_models = """
version: 2
snapshots:
- name: example_snapshot
  columns:
  - name: new_col
    tests:
    - not_null
"""

seeds__example_seed_csv = """a,b,c
1,2,3
4,5,6
7,8,9
"""
