{{ config(enabled=var('ad_reporting__google_ads_enabled', True)) }}

with base as (

    select *
    from {{ ref('stg_google_ads__campaign_network_state_stats_tmp') }}

),

fields as (

    select
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_google_ads__campaign_network_state_stats_tmp')),
                staging_columns=get_campaign_network_state_stats_columns()
            )
        }}

    from base
),

states as (

    select
      *
    from {{ ref('google_ads__geo_target_states') }}

),

final as (

    select
        fields.customer_id as account_id,
        fields.campaign_id,
        fields.campaign_name,
        fields.date as date_day,
        fields.ad_network_type,
        fields.geo_target_state as geo_target_state_id,
        case when states.country_code = 'US' then states.state_name else 'Non-US' end as state_name,
        fields.customer_currency_code,
        fields.clicks,
        fields.cost_micros / 1000000.0 as spend,
        fields.impressions
        {{ fivetran_utils.fill_pass_through_columns('google_ads__ad_stats_passthrough_metrics') }}

    from fields
    left join states on
      regexp_substr(fields.geo_target_state, '\\d+') = states.geo_target_state_id
)

select * from final
