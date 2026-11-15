{#
  The few places where DuckDB SQL and Spark SQL differ. Each macro has a
  default (DuckDB) and a spark__ version; dbt picks one per target
  (dbt-databricks dispatches to spark__ macros too).
#}

{# Seconds since the epoch, with microseconds: Spark's unix_timestamp() drops them. #}
{% macro epoch_seconds(ts) %}{{ return(adapter.dispatch('epoch_seconds')(ts)) }}{% endmacro %}
{% macro default__epoch_seconds(ts) %}(epoch_us(CAST({{ ts }} AS TIMESTAMP)) / 1000000.0){% endmacro %}
{% macro spark__epoch_seconds(ts) %}(unix_micros(CAST({{ ts }} AS TIMESTAMP)) / 1000000.0){% endmacro %}

{# Collapse runs of whitespace (DuckDB replaces only the first match without 'g'). #}
{% macro collapse_spaces(text) %}{{ return(adapter.dispatch('collapse_spaces')(text)) }}{% endmacro %}
{% macro default__collapse_spaces(text) %}regexp_replace({{ text }}, '\s+', ' ', 'g'){% endmacro %}
{% macro spark__collapse_spaces(text) %}regexp_replace({{ text }}, '\\s+', ' '){% endmacro %}

{# Sorted array to 'a,b,c'. #}
{% macro sorted_join(array, separator=",") %}{{ return(adapter.dispatch('sorted_join')(array, separator)) }}{% endmacro %}
{% macro default__sorted_join(array, separator) %}array_to_string(list_sort({{ array }}), '{{ separator }}'){% endmacro %}
{% macro spark__sorted_join(array, separator) %}array_join(array_sort({{ array }}), '{{ separator }}'){% endmacro %}

{# Median over a window frame: Spark rejects a frame on median(); percentile(x, 0.5) is the same value. #}
{% macro windowed_median(expression, window) %}{{ return(adapter.dispatch('windowed_median')(expression, window)) }}{% endmacro %}
{% macro default__windowed_median(expression, window) %}median({{ expression }}) OVER {{ window }}{% endmacro %}
{% macro spark__windowed_median(expression, window) %}percentile({{ expression }}, 0.5) OVER {{ window }}{% endmacro %}

{# Incremental strategy that replaces rows by unique key on each engine. #}
{% macro replace_strategy() %}{{ return('merge' if target.type == 'databricks' else 'delete+insert') }}{% endmacro %}

{# An empty array of strings. #}
{% macro empty_string_array() %}{{ return(adapter.dispatch('empty_string_array')()) }}{% endmacro %}
{% macro default__empty_string_array() %}CAST([] AS VARCHAR[]){% endmacro %}
{% macro spark__empty_string_array() %}CAST(array() AS ARRAY<STRING>){% endmacro %}
