{{ config(schema='odin_qa', materialized='incremental', unique_key='nk_superset_chart_type', sort='nk_superset_chart_type', dist='nk_superset_chart_type', tags=["superset"]) }}

with know_charts as (
    select 'country_map' as nk_superset_chart_type, 'Country Map' as chart_type_name
    union 
    select 'funnel' as nk_superset_chart_type, 'Funnel Chart' as chart_type_name
    union 
    select 'echarts_timeseries_bar' as nk_superset_chart_type, 'Time-series Bar v2' as chart_type_name
    union 
    select 'echarts_timeseries_line' as nk_superset_chart_type, 'Time-series Line v2' as chart_type_name
    union 
    select 'bar' as nk_superset_chart_type, 'Time-series Bar Chart' as chart_type_name
    union 
    select 'echarts_timeseries' as nk_superset_chart_type, 'Time-series Chart' as chart_type_name
    union 
    select 'table' as nk_superset_chart_type, 'Table' as chart_type_name
    union 
    select 'partition' as nk_superset_chart_type, 'Partition Chart' as chart_type_name
    union 
    select 'echarts_area' as nk_superset_chart_type, 'Area Chart' as chart_type_name
    union 
    select 'time_table' as nk_superset_chart_type, 'Time Table' as chart_type_name
    union 
    select 'cal_heatmap' as nk_superset_chart_type, 'Calendar Heatmap ' as chart_type_name
    union 
    select 'filter_box' as nk_superset_chart_type, 'Filter Box' as chart_type_name
    union 
    select 'pivot_table' as nk_superset_chart_type, 'Pivot Table' as chart_type_name
    union 
    select 'dist_bar' as nk_superset_chart_type, 'Bar Chart' as chart_type_name
    union 
    select 'line' as nk_superset_chart_type, 'Line Chart' as chart_type_name
    union 
    select 'deck_arc' as nk_superset_chart_type, 'deck.gl Arc' as chart_type_name
    union 
    select 'sunburst' as nk_superset_chart_type, 'Sunburst Chart' as chart_type_name
    union 
    select 'pie' as nk_superset_chart_type, 'Pie Chart' as chart_type_name
    union 
    select 'pivot_table_v2' as nk_superset_chart_type, 'Pivot Table v2' as chart_type_name
    union 
    select 'histogram' as nk_superset_chart_type, 'Histogram Chart' as chart_type_name
    union 
    select 'treemap' as nk_superset_chart_type, 'Treemap Chart' as chart_type_name
    union 
    select 'word_cloud' as nk_superset_chart_type, 'Word Cloud' as chart_type_name
    union 
    select 'big_number_total' as nk_superset_chart_type, 'Big Number' as chart_type_name
    union 
    select 'gauge_chart' as nk_superset_chart_type, 'Gauge Chart' as chart_type_name
    union 
    select 'echarts_timeseries_smooth' as nk_superset_chart_type, 'Time-series Smooth' as chart_type_name
    union 
    select 'mixed_timeseries' as nk_superset_chart_type, 'Mixed Time Series' as chart_type_name
    union 
    select 'big_number' as nk_superset_chart_type, 'Big Number with trendline' as chart_type_name
), all_chart_types as (
    select distinct viz_type
    from {{ ref('stg_superset_slices') }}

)

SELECT
    row_number() over (order by s.viz_type ) as sk_superset_chart_type, 
    s.viz_type as nk_superset_chart_type,
    nvl(k.chart_type_name, s.viz_type) as chart_type_name,
    convert_timezone('America/New_York', getdate()) AS updated_at
FROM all_chart_types as s
LEFT JOIN  know_charts as k on s.viz_type = k.nk_superset_chart_type

{% if is_incremental() %}

where s.viz_type not in (select distinct nk_superset_chart_type from {{this}})

{% endif %}