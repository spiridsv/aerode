-- TODO: add SCD
insert into {dst}
with stg as (select *
             from NHL_STG.STATSAPI_JSON
             where tech_uuid = '{uuid}'
               and api_id = {api_id}
               and JSON_EXISTS(json, '$.stats[*].splits[*].team.id') = 1
             order by tech_load_ts desc
             limit 1
             ),
     ods as (select gccMurmurHash(json) as hash
             from NHL_ODS.STATSAPI_JSON
             where api_id = {api_id})
select *
from stg
where gccMurmurHash(json) not in (select hash from ods)
;