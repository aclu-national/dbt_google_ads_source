{{ config(enabled=var('ad_reporting__google_ads_enabled', True)) }}

select *
from {{ var('bpi_backfill__campaign_network_state_stats') }}
