select 
 record,             -- the API object, untouched (VARIANT)
    business_date,   -- dt= partition of the S3 file
    run_id,          -- which pipeline run pulled it
    source_file,     -- S3 file (lineage)
    file_row,        -- line in that file (debugging)
    loaded_at        -- when Snowpipe loaded it (dedup tie-breaker in silver)
from {{source('policy_admin_api','claims')}}

