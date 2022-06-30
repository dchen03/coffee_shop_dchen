{{ codegen.generate_base_model(
    source_name='github',
    table_name='issue'
) }}

{{ codegen.generate_base_model(
    source_name='github',
    table_name='pull_request'
) }}

{{ codegen.generate_base_model(
    source_name='github',
    table_name='repository'
) }}