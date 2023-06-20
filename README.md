# h/w aero de

## Task

Extract data from [https://statsapi.web.nhl.com/api/v1/teams/21/stats]("https://statsapi.web.nhl.com/api/v1/teams/21/stats")
and Load to Clickhouse w/o Transform.

## deploy compose

```shell
docker-compose up
```

Check access to [Airflow](http://127.0.0.1:8080/)

## run DAGs

1. Run test DAG [TEST_DAG_CLICKHOUSE](http://127.0.0.1:8080/dags/TEST_DAG_CLICKHOUSE/)
to check connections to Clickhouse
2. Run DAG [NHL_TO_CH](http://127.0.0.1:8080/dags/NHL_TO_CH/) to start EL process (scheduler `0 */12 * * *`)
3. Check data from Clickhouse

```sql
select *
from NHL_ODS.STATS_API_STATS_SINGLE_SEASON_V
         inner join NHL_ODS.STATS_API_REGULAR_SEASON_STAT_RANKINGS_V using (team_id);
```

sample output (jsoned from DB):
```json
[
  {
    "api_id": "21",
    "team_id": "21",
    "team_name": "Colorado Avalanche",
    "team_link": "/api/v1/teams/21",
    "gamesPlayed": "82",
    "wins": "51",
    "losses": "24",
    "ot": "7",
    "pts": "109",
    "ptPctg": "66.5",
    "goalsPerGame": 3.341,
    "goalsAgainstPerGame": 2.72,
    "evGGARatio": 1.2,
    "powerPlayPercentage": "24.5",
    "powerPlayGoals": 64,
    "powerPlayGoalsAgainst": 52,
    "powerPlayOpportunities": 261,
    "penaltyKillPercentage": "79.0",
    "shotsPerGame": 33.2561,
    "shotsAllowed": 30.9634,
    "winScoreFirst": 0.774,
    "winOppScoreFirst": 0.321,
    "winLeadFirstPer": 0.861,
    "winLeadSecondPer": 0.837,
    "winOutshootOpp": 0.674,
    "winOutshotByOpp": 0.531,
    "faceOffsTaken": 4830,
    "faceOffsWon": 2256,
    "faceOffsLost": 2574,
    "faceOffWinPercentage": 46.7,
    "shootingPctg": 10,
    "savePctg": 0.912,
    "tech_load_ts": "2023-06-20 06:50:39",
    "tech_uuid": "03165f3f-b98b-457c-aca3-511c18a5a514",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.api_id": "21",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.team_name": "Colorado Avalanche",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.team_link": "/api/v1/teams/21",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.wins": "5th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.losses": "9th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.ot": "24th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.pts": "7th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.ptPctg": "7th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.goalsPerGame": "10th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.goalsAgainstPerGame": "9th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.evGGARatio": "8th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.powerPlayPercentage": "6th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.powerPlayGoals": "6th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.powerPlayGoalsAgainst": "15th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.powerPlayOpportunities": "12th",
    "penaltyKillOpportunities": "17th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.penaltyKillPercentage": "17th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.shotsPerGame": "8th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.shotsAllowed": "14th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.winScoreFirst": "2nd",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.winOppScoreFirst": "27th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.winLeadFirstPer": "5th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.winLeadSecondPer": "21st",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.winOutshootOpp": "4th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.winOutshotByOpp": "4th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.faceOffsTaken": "7th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.faceOffsWon": "21st",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.faceOffsLost": "31st",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.faceOffWinPercentage": "28th",
    "savePctRank": "6th",
    "shootingPctRank": "17th",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.tech_load_ts": "2023-06-20 06:50:39",
    "STATS_API_REGULAR_SEASON_STAT_RANKINGS_V.tech_uuid": "03165f3f-b98b-457c-aca3-511c18a5a514"
  }
]
```
