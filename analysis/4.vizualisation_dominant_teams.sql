-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Dominant Formula 1 Teams of All Time</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_dominant_teams
as
select 
    team_name, count(1) as total_races, 
    sum(calculated_points) as total_points,
    avg(calculated_points) as avg_points,
    rank() over(order by avg(calculated_points) desc) team_rank
  from f1_presentation.calculated_race_results
group by team_name
having count(1) >= 100
order by total_points desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Tableau met en exergue le classement des saisons les plus dominatrices réalisées par un écurie
-- MAGIC - 1ère visualiation (5 meilleures écuries) = évolution des dominations d'écurie par saison
-- MAGIC - 2ème visualiation = l'écurie ayant remporté le plus de points et son nombre total de course

-- COMMAND ----------

-- MAGIC %python
-- MAGIC html = """<h3 style="color:Black;text-align:center;font-family:Ariel">Line chart and Bar chart </h3>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

select race_year,
    team_name, 
    count(1) as total_races, 
    sum(calculated_points) as total_points,
    avg(calculated_points) as avg_points
  from f1_presentation.calculated_race_results
  where team_name in (select team_name from v_dominant_teams where team_rank <= 5)
group by race_year, team_name
order by race_year, avg_points desc

-- COMMAND ----------

-- MAGIC %python
-- MAGIC html = """<h3 style="color:Black;text-align:center;font-family:Ariel">Area chart</h3>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

select race_year,
    team_name, 
    count(1) as total_races, 
    sum(calculated_points) as total_points,
    avg(calculated_points) as avg_points
  from f1_presentation.calculated_race_results
  where team_name in (select team_name from v_dominant_teams where team_rank <= 5)
group by race_year, team_name
order by race_year, avg_points desc

-- COMMAND ----------

