{{ config(enabled=var('ad_reporting__google_ads_enabled', True)) }}

with base as (

    select *
    from {{ ref('stg_google_ads__bpi_backfill__campaign_network_state_stats_tmp') }}

),

fields as (

    select
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_google_ads__bpi_backfill__campaign_network_state_stats_tmp')),
                staging_columns=get_bpi_backfill__campaign_network_state_stats_columns()
            )
        }}

    from base
),

final as (

    select
        fields.account_id::bigint as account_id,
        fields.campaign_id::bigint as campaign_id,
        fields.campaign as campaign_name,
        fields.day::date as date_day,
        case fields.campaign_type -- https://developers.google.com/google-ads/api/reference/rpc/v11/AdNetworkTypeEnum.AdNetworkType
          when 'Display' then 'CONTENT'
          when 'Search' then 'SEARCH'
          when 'Video' then 'YOUTUBE_WATCH'
        end as ad_network_type,
        fields.state_matched as state_name,
        fields.currency_code as customer_currency_code,
        fields.clicks as clicks,
        fields.cost as spend,
        fields.impr as impressions
        {{ fivetran_utils.fill_pass_through_columns('google_ads__ad_stats_passthrough_metrics') }}
    from fields
)

select * from final
