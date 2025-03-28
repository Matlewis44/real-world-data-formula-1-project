-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers of All Time</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_dominant_drivers
as
select 
    driver_name, count(1) as total_races, 
    sum(calculated_points) as total_points,
    avg(calculated_points) as avg_points,
    rank() over(order by avg(calculated_points) desc) driver_rank
  from f1_presentation.calculated_race_results
group by driver_name
having count(1) >= 50
order by total_points desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Tableau met en exergue le classement des saisons les plus dominatrices réalisées par un pilote
-- MAGIC - 1ère visualiation = domination du pilote par saison
-- MAGIC - 2ème visualiation = le pilotes ayant remporté le plus de points et son nombre total de course

-- COMMAND ----------

select race_year,
    driver_name, 
    count(1) as total_races, 
    sum(calculated_points) as total_points,
    avg(calculated_points) as avg_points
  from f1_presentation.calculated_race_results
  where driver_name in (select driver_name from  where driver_rank <= 10 and avg_points >= 9)
group by race_year, driver_name
order by race_year, avg_points desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Reste à executer et visualiser

-- COMMAND ----------

select race_year,
    driver_name, 
    count(1) as total_races, 
    sum(calculated_points) as total_points,
    avg(calculated_points) as avg_points
  from f1_presentation.calculated_race_results
  where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10)
group by race_year, driver_name
order by race_year, avg_points desc

-- COMMAND ----------

select * from f1_presentation.calculated_race_results