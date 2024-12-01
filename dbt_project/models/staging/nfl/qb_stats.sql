{{config(alias='qb_stats')}}

select 
    PlayerName as player_name,
    PlayerId as player_id, 
    Team as team,
    PlayerOpponent as opponent,
    PassingYDS as passing_yards,
    PassingTD as passing_touchdowns,
    PassingInt as interceptions,
    RushingYDS as rushing_yards,
    ReceivingYDS as receiving_yards,
    Fum as fumbles,
    TotalPoints as fantasy_points,
    week,
from {{ source('nfl', 'players') }}