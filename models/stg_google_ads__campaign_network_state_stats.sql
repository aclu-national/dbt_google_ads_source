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
    from {{ ref('google_ads__state_mappings') }}

),

bpi as (

    select
      *
    from {{ ref('stg_google_ads__bpi_backfill__campaign_network_state_stats') }}

),

fivetran as (

    select
        fields.customer_id::bigint as account_id,
        fields.campaign_id::bigint as campaign_id,
        fields.campaign_name,
        fields.date as date_day,
        fields.ad_network_type,
        states.name as state_name,
        fields.customer_currency_code,
        fields.clicks,
        fields.cost_micros / 1000000.0 as spend,
        fields.impressions
        {{ fivetran_utils.fill_pass_through_columns('google_ads__ad_stats_passthrough_metrics') }}

    from fields
    left join states on
      regexp_substr(fields.geo_target_state, '\\d+') = states.criteria_id
),

unioned as (

    select *, 'Backfill' as sync_source from bpi

      union all

    select *, 'Fivetran' as sync_source from fivetran

),

final as (

    select
      account_id,
      campaign_id,
      campaign_name,
      sync_source,
      date_day,
      ad_network_type,
      state_name,
      customer_currency_code,
      sum(clicks) as clicks,
      sum(spend) as spend,
      sum(impressions) as impressions
    from unioned
    {{ dbt_utils.group_by(n = 8) }}

)
select * from final
