-- DEBUG:
-- DROP DATABASE IF EXISTS NHL_ODS;
-- DROP DATABASE IF EXISTS NHL_STG;

/*
 STG LAYER
 */

CREATE DATABASE IF NOT EXISTS NHL_STG;

CREATE TABLE IF NOT EXISTS NHL_STG.STATSAPI_JSON
(
    json         String,
    api_id       UInt16 comment 'API id',
    tech_uuid    UUID comment 'tech UUID from EL',
    tech_load_ts DATETIME comment 'tech load timestamp'
) ENGINE MergeTree() ORDER BY tuple()
      TTL tech_load_ts + INTERVAL 1 WEEK DELETE; -- save our disks;)


/*
 ODS LAYER
 */
CREATE DATABASE IF NOT EXISTS NHL_ODS;

CREATE TABLE IF NOT EXISTS NHL_ODS.STATSAPI_JSON
(
    json         String,
    api_id       UInt16 comment 'API id',
    tech_uuid    UUID comment 'tech UUID from EL',
    tech_load_ts DateTime default now() comment 'tech load timestamp'
) ENGINE MergeTree ORDER BY (api_id,tech_load_ts);

-- VIEW

create or replace view NHL_ODS.STATSAPI_JSON_TYPED_V
as
with flat_l1 as (select arr, api_id, tech_uuid, tech_load_ts
                 from (select JSONExtractArrayRaw(json, 'stats') as arr,
                              api_id,
                              tech_load_ts,
                              tech_uuid
                       from NHL_ODS.STATSAPI_JSON
                       order by tech_load_ts desc
                       limit 1 by api_id
                          )
                          array join arr)
select api_id,
       JSONExtractRaw(arr, 'type')            as type,
       JSONExtractString(type, 'displayName') as displayName,
       JSONExtractRaw(type, 'gameType')       as gameType,
       JSONExtractArrayRaw(arr, 'splits')[1]  as splits, -- only 1 dict
       JSONExtractRaw(splits, 'stat')         as stat,
       JSONExtractRaw(splits, 'team')         as team,
       tech_load_ts,
       tech_uuid
from flat_l1
;

-- statsSingleSeason
create or replace view NHL_ODS.STATS_API_STATS_SINGLE_SEASON_V
as
select api_id,
       JSONExtractUInt(team, 'id')                      as team_id,                -- PK
       JSONExtractString(team, 'name')                  as team_name,
       JSONExtractString(team, 'link')                  as team_link,
       JSONExtractUInt(stat, 'gamesPlayed')             as gamesPlayed,            -- : 82,
       JSONExtractUInt(stat, 'wins')                    as wins,                   -- : 51,
       JSONExtractUInt(stat, 'losses')                  as losses,                 -- : 24,
       JSONExtractUInt(stat, 'ot')                      as ot,                     -- : 7,
       JSONExtractUInt(stat, 'pts')                     as pts,                    -- : 109,
       JSONExtractString(stat, 'ptPctg')                as ptPctg,                 -- : "66.5",
       JSONExtractFloat(stat, 'goalsPerGame')           as goalsPerGame,           -- : 3.341,
       JSONExtractFloat(stat, 'goalsAgainstPerGame')    as goalsAgainstPerGame,    -- : 2.72,
       JSONExtractFloat(stat, 'evGGARatio')             as evGGARatio,             -- : 1.2,
       JSONExtractString(stat, 'powerPlayPercentage')   as powerPlayPercentage,    -- : "24.5",
       JSONExtractFloat(stat, 'powerPlayGoals')         as powerPlayGoals,         -- : 64.0,
       JSONExtractFloat(stat, 'powerPlayGoalsAgainst')  as powerPlayGoalsAgainst,  -- : 52.0,
       JSONExtractFloat(stat, 'powerPlayOpportunities') as powerPlayOpportunities, -- : 261.0,
       JSONExtractString(stat, 'penaltyKillPercentage') as penaltyKillPercentage,  -- : "79.0",
       JSONExtractFloat(stat, 'shotsPerGame')           as shotsPerGame,           -- : 33.2561,
       JSONExtractFloat(stat, 'shotsAllowed')           as shotsAllowed,           -- : 30.9634,
       JSONExtractFloat(stat, 'winScoreFirst')          as winScoreFirst,          -- : 0.774,
       JSONExtractFloat(stat, 'winOppScoreFirst')       as winOppScoreFirst,       -- : 0.321,
       JSONExtractFloat(stat, 'winLeadFirstPer')        as winLeadFirstPer,        -- : 0.861,
       JSONExtractFloat(stat, 'winLeadSecondPer')       as winLeadSecondPer,       -- : 0.837,
       JSONExtractFloat(stat, 'winOutshootOpp')         as winOutshootOpp,         -- : 0.674,
       JSONExtractFloat(stat, 'winOutshotByOpp')        as winOutshotByOpp,        -- : 0.531,
       JSONExtractFloat(stat, 'faceOffsTaken')          as faceOffsTaken,          -- : 4830.0,
       JSONExtractFloat(stat, 'faceOffsWon')            as faceOffsWon,            -- : 2256.0,
       JSONExtractFloat(stat, 'faceOffsLost')           as faceOffsLost,           -- : 2574.0,
       JSONExtractFloat(stat, 'faceOffWinPercentage')   as faceOffWinPercentage,   -- : "46.7",
       JSONExtractFloat(stat, 'shootingPctg')           as shootingPctg,           -- : 10.0,
       JSONExtractFloat(stat, 'savePctg')               as savePctg,               -- : 0.912
       tech_load_ts,
       tech_uuid
from NHL_ODS.STATSAPI_JSON_TYPED_V
where displayName = 'statsSingleSeason';


--- regularSeasonStatRankings
create or replace view NHL_ODS.STATS_API_REGULAR_SEASON_STAT_RANKINGS_V
as
select api_id,
       JSONExtractUInt(team, 'id')                         as team_id,                  -- PK
       JSONExtractString(team, 'name')                     as team_name,
       JSONExtractString(team, 'link')                     as team_link,
       JSONExtractString(stat, 'wins')                     as wins,                     -- : "5th",
       JSONExtractString(stat, 'losses')                   as losses,                   -- : "9th",
       JSONExtractString(stat, 'ot')                       as ot,                       -- : "24th",
       JSONExtractString(stat, 'pts')                      as pts,                      -- : "7th",
       JSONExtractString(stat, 'ptPctg')                   as ptPctg,                   -- : "7th",
       JSONExtractString(stat, 'goalsPerGame')             as goalsPerGame,             -- : "10th",
       JSONExtractString(stat, 'goalsAgainstPerGame')      as goalsAgainstPerGame,      -- : "9th",
       JSONExtractString(stat, 'evGGARatio')               as evGGARatio,               -- : "8th",
       JSONExtractString(stat, 'powerPlayPercentage')      as powerPlayPercentage,      -- : "6th",
       JSONExtractString(stat, 'powerPlayGoals')           as powerPlayGoals,           -- : "6th",
       JSONExtractString(stat, 'powerPlayGoalsAgainst')    as powerPlayGoalsAgainst,    -- : "15th",
       JSONExtractString(stat, 'powerPlayOpportunities')   as powerPlayOpportunities,   -- : "12th",
       JSONExtractString(stat, 'penaltyKillOpportunities') as penaltyKillOpportunities, -- : "17th",
       JSONExtractString(stat, 'penaltyKillPercentage')    as penaltyKillPercentage,    -- : "17th",
       JSONExtractString(stat, 'shotsPerGame')             as shotsPerGame,             -- : "8th",
       JSONExtractString(stat, 'shotsAllowed')             as shotsAllowed,             -- : "14th",
       JSONExtractString(stat, 'winScoreFirst')            as winScoreFirst,            -- : "2nd",
       JSONExtractString(stat, 'winOppScoreFirst')         as winOppScoreFirst,         -- : "27th",
       JSONExtractString(stat, 'winLeadFirstPer')          as winLeadFirstPer,          -- : "5th",
       JSONExtractString(stat, 'winLeadSecondPer')         as winLeadSecondPer,         -- : "21st",
       JSONExtractString(stat, 'winOutshootOpp')           as winOutshootOpp,           -- : "4th",
       JSONExtractString(stat, 'winOutshotByOpp')          as winOutshotByOpp,          -- : "4th",
       JSONExtractString(stat, 'faceOffsTaken')            as faceOffsTaken,            -- : "7th",
       JSONExtractString(stat, 'faceOffsWon')              as faceOffsWon,              -- : "21st",
       JSONExtractString(stat, 'faceOffsLost')             as faceOffsLost,             -- : "31st",
       JSONExtractString(stat, 'faceOffWinPercentage')     as faceOffWinPercentage,     -- : "28th",
       JSONExtractString(stat, 'savePctRank')              as savePctRank,              -- : "6th",
       JSONExtractString(stat, 'shootingPctRank')          as shootingPctRank,          -- : "17th"
       tech_load_ts,
       tech_uuid
from NHL_ODS.STATSAPI_JSON_TYPED_V
where displayName = 'regularSeasonStatRankings';