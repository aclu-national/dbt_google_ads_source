{% macro get_bpi_backfill__campaign_network_state_stats_columns() %}

{% set columns = [
    {"name": "account_id", "datatype": dbt_utils.type_int()},
    {"name": "day", "datatype": "date"},
    {"name": "campaign_id", "datatype": dbt_utils.type_int()},
    {"name": "campaign", "datatype": dbt_utils.type_string()},
    {"name": "clicks", "datatype": dbt_utils.type_int()},
    {"name": "cost", "datatype": dbt_utils.type_int()},
    {"name": "impr", "datatype": dbt_utils.type_int()},
    {"name": "campaign_type", "datatype": dbt_utils.type_string()},
    {"name": "state_matched", "datatype": dbt_utils.type_string()},
    {"name": "currency_code", "datatype": dbt_utils.type_string()}
] %}

{{ fivetran_utils.add_pass_through_columns(columns, var('google_ads__ad_stats_passthrough_metrics')) }}

{{ return(columns) }}

{% endmacro %}