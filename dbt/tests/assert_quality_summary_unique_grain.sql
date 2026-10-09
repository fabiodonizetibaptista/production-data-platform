select
    pickup_month,
    quality_issue,
    count(*) as row_count

from {{ ref('mart_data_quality_summary') }}

group by
    pickup_month,
    quality_issue

having count(*) > 1
