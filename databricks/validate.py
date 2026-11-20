# Databricks notebook source
"""Independent post-run checks for the MovieLens pipeline."""
import json
from pyspark.sql import functions as F

# COMMAND ----------
dbutils.widgets.text('catalog', 'workspace')
dbutils.widgets.text('schema', 'cinematch')
catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')
if not all(x.replace('_', '').isalnum() for x in (catalog, schema)):
    raise ValueError('Invalid catalog or schema')
ns = f'`{catalog}`.`{schema}`'
root = f'/Volumes/{catalog}/{schema}/data/exports'

def table(name):
    return spark.table(f'{ns}.{name}')

expected = {'movies': 87585, 'ratings': 32000204, 'tags': 2000072}
quality = {r['dataset']: r.asDict() for r in table('gold_quality_report').collect()}
for name, count in expected.items():
    assert quality[name]['bronze_rows'] == count, (name, quality[name])
for name in ('movies', 'ratings', 'tags'):
    silver_count = table('silver_' + name).count()
    parquet_count = spark.read.parquet(f'{root}/{name}.parquet').count()
    assert silver_count == quality[name]['silver_rows'] == parquet_count, (name, silver_count, parquet_count)

movies = table('silver_movies')
ratings = table('silver_ratings')
tags = table('silver_tags')
assert movies.groupBy('movieId').count().where('count > 1').limit(1).count() == 0
assert ratings.where('userId IS NULL OR movieId IS NULL OR rating IS NULL OR timestamp IS NULL OR rated_at IS NULL OR userId <= 0 OR movieId <= 0 OR rating < 0.5 OR rating > 5 OR rating * 2 != floor(rating * 2) OR timestamp <= 0').limit(1).count() == 0
assert tags.where("userId IS NULL OR movieId IS NULL OR tag IS NULL OR tag = '' OR timestamp IS NULL OR tagged_at IS NULL OR userId <= 0 OR movieId <= 0 OR timestamp <= 0").limit(1).count() == 0
for name, frame in [('ratings', ratings), ('tags', tags)]:
    assert frame.join(movies.select('movieId'), 'movieId', 'left_anti').limit(1).count() == 0, name
assert table('gold_interactions').count() == quality['ratings']['silver_rows']
print(json.dumps({'quality': quality, 'checks': 'passed'}, indent=2))
dbutils.notebook.exit(json.dumps({'quality': quality, 'checks': 'passed'}))
