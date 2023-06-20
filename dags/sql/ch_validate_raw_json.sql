-- TODO: add validate by JSON_VALUE
select isValidJSON(json)
from {src}
where tech_uuid = '{uuid}'
  and api_id = {api_id}
  and JSON_EXISTS(json, '$.stats[*].splits[*].team.id') = 1;