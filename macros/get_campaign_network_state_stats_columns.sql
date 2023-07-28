{% macro get_campaign_network_state_stats_columns() %}

{% set columns = [
    {"name": "customer_id", "datatype": dbt.type_int()},
    {"name": "date", "datatype": "date"},
    {"name": "campaign_id", "datatype": dbt.type_int()},
    {"name": "campaign_name", "datatype": dbt.type_string()},
    {"name": "clicks", "datatype": dbt.type_int()},
    {"name": "cost_micros", "datatype": dbt.type_int()},
    {"name": "impressions", "datatype": dbt.type_int()},
    {"name": "ad_network_type", "datatype": dbt.type_string()},
    {"name": "geo_target_state", "datatype": dbt.type_string()},
    {"name": "customer_currency_code", "datatype": dbt.type_string()}
] %}

{{ fivetran_utils.add_pass_through_columns(columns, var('google_ads__ad_stats_passthrough_metrics')) }}

{{ return(columns) }}

{% endmacro %}
