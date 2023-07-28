{% macro get_keyword_stats_columns() %}

{% set columns = [
    {"name": "_fivetran_id", "datatype": dbt_utils.type_string()},
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
    {"name": "ad_group_criterion_criterion_id", "datatype": dbt_utils.type_int()},
    {"name": "ad_group_id", "datatype": dbt_utils.type_int()},
    {"name": "ad_network_type", "datatype": dbt_utils.type_string()},
    {"name": "campaign_id", "datatype": dbt_utils.type_int()},
    {"name": "clicks", "datatype": dbt_utils.type_int()},
    {"name": "cost_micros", "datatype": dbt_utils.type_int()},
    {"name": "customer_id", "datatype": dbt_utils.type_int()},
    {"name": "date", "datatype": "date"},
    {"name": "device", "datatype": dbt_utils.type_string()},
    {"name": "impressions", "datatype": dbt_utils.type_int()}
] %}

{{ fivetran_utils.add_pass_through_columns(columns, var('google_ads__keyword_stats_passthrough_metrics')) }}

{{ return(columns) }}

{% endmacro %}
