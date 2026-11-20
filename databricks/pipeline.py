# Databricks notebook source
"""MovieLens 32M batch pipeline. Run on Databricks with Unity Catalog."""

import os
import hashlib
import re
import shutil
import tempfile
import urllib.request
import zipfile
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------
dbutils.widgets.text('catalog', 'workspace')
dbutils.widgets.text('schema', 'cinematch')
catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')
if not all(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', x) for x in (catalog, schema)):
    raise ValueError('Catalog and schema must be simple SQL identifiers')
namespace = f'`{catalog}`.`{schema}`'
spark.conf.set('spark.sql.session.timeZone', 'UTC')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {namespace}')
spark.sql(f'CREATE VOLUME IF NOT EXISTS {namespace}.data')
root = f'/Volumes/{catalog}/{schema}/data'
raw = f'{root}/raw/ml-32m'

# COMMAND ----------
# Download on the driver and extract only the four expected files; no arbitrary ZIP paths.
checksums = {
    'links.csv': '8f033867bcb4e6be8792b21468b4fa6e',
    'movies.csv': '0df90835c19151f9d819d0822e190797',
    'ratings.csv': 'cf12b74f9ad4b94a011f079e26d4270a',
    'tags.csv': '963bf4fa4de6b8901868fddd3eb54567',
}
files = list(checksums)
with tempfile.TemporaryDirectory() as tmp:
    archive = os.path.join(tmp, 'ml-32m.zip')
    urllib.request.urlretrieve('https://files.grouplens.org/datasets/movielens/ml-32m.zip', archive)
    dbutils.fs.mkdirs(raw)
    with zipfile.ZipFile(archive) as z:
        for name in files:
            with z.open(f'ml-32m/{name}') as source, open(os.path.join(tmp, name), 'wb') as target:
                shutil.copyfileobj(source, target)
            digest = hashlib.md5()  # Dataset integrity check published by GroupLens.
            with open(os.path.join(tmp, name), 'rb') as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b''):
                    digest.update(chunk)
            if digest.hexdigest() != checksums[name]:
                raise ValueError(f'Checksum mismatch for {name}')
            dbutils.fs.cp('file:' + os.path.join(tmp, name), f'{raw}/{name}')

# COMMAND ----------
columns = {
    'movies': ['movieId', 'title', 'genres'],
    'ratings': ['userId', 'movieId', 'rating', 'timestamp'],
    'tags': ['userId', 'movieId', 'tag', 'timestamp'],
    'links': ['movieId', 'imdbId', 'tmdbId'],
}
for name, fields in columns.items():
    input_schema = StructType([StructField(c, StringType(), True) for c in fields])
    df = (spark.read.schema(input_schema).option('header', True)
          .option('multiLine', name in ('movies', 'tags')).option('escape', '"').csv(f'{raw}/{name}.csv'))
    df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{namespace}.bronze_{name}')

def table(layer, name):
    return spark.table(f'{namespace}.{layer}_{name}')

def save(df, layer, name):
    df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{namespace}.{layer}_{name}')

# COMMAND ----------
movies = (table('bronze', 'movies').select(
    F.expr('try_cast(movieId as int)').alias('movieId'), F.trim('title').alias('title'),
    F.trim('genres').alias('genres'))
    .where('movieId > 0 AND title IS NOT NULL AND title != "" AND genres IS NOT NULL')
    .dropDuplicates())
# Conflicting metadata is a source-quality error: do not select an arbitrary record.
if movies.groupBy('movieId').count().where('count > 1').limit(1).count():
    raise ValueError('Conflicting movie records share a movieId')
save(movies, 'silver', 'movies')
movies = table('silver', 'movies')

ratings = (table('bronze', 'ratings').select(
    F.expr('try_cast(userId as int)').alias('userId'), F.expr('try_cast(movieId as int)').alias('movieId'),
    F.expr('try_cast(rating as double)').alias('rating'), F.expr('try_cast(timestamp as bigint)').alias('timestamp'))
    .where('userId > 0 AND movieId > 0 AND rating >= 0.5 AND rating <= 5 AND rating * 2 = floor(rating * 2) AND timestamp > 0')
    .join(movies.select('movieId'), 'movieId', 'left_semi').dropDuplicates()
    .withColumn('rated_at', F.timestamp_seconds('timestamp')))
save(ratings, 'silver', 'ratings')

tags = (table('bronze', 'tags').select(
    F.expr('try_cast(userId as int)').alias('userId'), F.expr('try_cast(movieId as int)').alias('movieId'),
    F.trim('tag').alias('tag'), F.expr('try_cast(timestamp as bigint)').alias('timestamp'))
    .where('userId > 0 AND movieId > 0 AND tag IS NOT NULL AND tag != "" AND timestamp > 0')
    .join(movies.select('movieId'), 'movieId', 'left_semi').dropDuplicates()
    .withColumn('tagged_at', F.timestamp_seconds('timestamp')))
save(tags, 'silver', 'tags')
links = (table('bronze', 'links').select(
    F.expr('try_cast(movieId as int)').alias('movieId'), F.trim('imdbId').alias('imdbId'),
    F.trim('tmdbId').alias('tmdbId')).where('movieId > 0')
    .join(movies.select('movieId'), 'movieId', 'left_semi').dropDuplicates())
if links.groupBy('movieId').count().where('count > 1').limit(1).count():
    raise ValueError('Conflicting links share a movieId')
save(links, 'silver', 'links')

# COMMAND ----------
ratings = table('silver', 'ratings')
save(ratings.groupBy('movieId').agg(F.count('*').alias('rating_count'),
    F.avg('rating').alias('mean_rating')), 'gold', 'movie_stats')
save(movies.select('movieId', F.explode(F.split('genres', r'\|')).alias('genre'))
    .where("genre != '(no genres listed)'"), 'gold', 'movie_genres')
save(ratings.select('userId', 'movieId', 'rating', 'timestamp', 'rated_at'), 'gold', 'interactions')

# COMMAND ----------
# Reconciliation includes exact duplicate removal and rejected records together.
quality = []
for name in columns:
    bronze_count = table('bronze', name).count()
    silver_count = table('silver', name).count()
    if bronze_count == 0 or silver_count == 0 or silver_count > bronze_count:
        raise ValueError(f'Invalid row counts for {name}: {bronze_count}, {silver_count}')
    quality.append((name, bronze_count, silver_count, bronze_count - silver_count))
report = spark.createDataFrame(quality, ['dataset', 'bronze_rows', 'silver_rows', 'removed_rows'])
save(report, 'gold', 'quality_report')
for name in ['movies', 'ratings', 'tags']:
    table('silver', name).write.mode('overwrite').parquet(f'{root}/exports/{name}.parquet')
print('Quality report:')
report.show(truncate=False)
print(f'Exports: {root}/exports/')
