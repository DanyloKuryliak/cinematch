# CineMatch

MovieLens 32M data engineering pipeline for a film recommendation project. Analytics, recommendation models and application development are separate team workstreams.

## Status

The full pipeline and an independent validation notebook completed successfully on Databricks on 20 November 2026. Analytics, models, and the application remain to be developed by their respective workstreams.

| Dataset | Bronze rows | Silver rows | Removed |
| --- | ---: | ---: | ---: |
| Movies | 87,585 | 87,585 | 0 |
| Ratings | 32,000,204 | 32,000,204 | 0 |
| Tags | 2,000,072 | 2,000,072 | 0 |
| Links | 87,585 | 87,585 | 0 |

The validation checked official counts for movies, ratings and tags; movie key uniqueness; required rating and tag fields, rating range and increments; referential integrity; interaction counts; and read-back counts for each Parquet export. Row-count reconciliation does not establish byte-for-byte equality between Silver and exports. See [validation](databricks/validate.py).

## Architecture

The Databricks notebook downloads the official MovieLens 32M archive, verifies the four files against the published MD5 checksums, and stages the CSV files in a Unity Catalog volume, writes raw Delta tables (`bronze_*`), cleans and validates them (`silver_*`), and generates Delta analytical tables (`gold_*`). Re-running replaces the outputs table by table; a failed run can leave a mixture of old and new outputs, so use the outputs only after a successful run. Gold movie statistics describe the entire dataset and must not be used as training features or popularity baselines without first restricting ratings to the training period.

Silver preserves original Unix timestamps and adds UTC timestamps. Exact duplicate rows and records with invalid required fields or unknown movie IDs are removed; conflicting movie or link records stop the pipeline. The quality report reconciles source and cleaned row counts but combines rejected and duplicate rows. `imdbId` is stored as text to preserve leading zeros. Ratings and tags may include repeated user/movie pairs when their other fields differ.

Exports are Spark Parquet **directories** at `exports/movies.parquet`, `exports/ratings.parquet`, and `exports/tags.parquet` in the volume. They can contain multiple part files and should be read as datasets. Source data and outputs are not committed to Git.

## Run

Import `databricks/pipeline.py` and `databricks/validate.py` as Databricks notebooks and run on a compatible compute with outbound access to GroupLens and Unity Catalog permissions to create a schema, managed volume and tables. Set notebook widgets `catalog` and `schema` to an authorized namespace; defaults are `workspace` and `cinematch`. The identity executing the notebook owns its created objects, so arrange project-scoped access before team use. The notebooks use the Python standard library and PySpark provided by Databricks; see `requirements.txt`. A complete run downloads the full dataset and can incur compute and storage charges. Verify the run, row counts, access and exports before relying on the results. If compute lacks outbound access, stage the official archive through an approved route and adapt ingestion.

The dataset is provided by [GroupLens](https://grouplens.org/datasets/movielens/32m/); consult the [dataset README and terms](https://files.grouplens.org/datasets/movielens/ml-32m-README.html) before redistribution. Cite Harper and Konstan, *The MovieLens Datasets: History and Context*, DOI [10.1145/2827872](https://doi.org/10.1145/2827872).

## Team workspace

The project namespace is `workspace.cinematch`, with volume `workspace.cinematch.data`. The shared notebooks are `/Shared/CineMatch/pipeline` and `/Shared/CineMatch/validate`; the saved job is `CineMatch - MovieLens data pipeline`. The validation task runs after the pipeline task. Run it from the Databricks Jobs UI or, using an authenticated profile:

```bash
databricks jobs run-now 373620772051605 -p ucu
```

To reproduce the saved job in another workspace, import both notebooks at the paths in `databricks/job.json`, then run `databricks jobs create --json @databricks/job.json -p <profile>`. Adjust paths and notebook widget defaults for the target workspace.

The schema includes `bronze_movies`, `bronze_ratings`, `bronze_tags`, `bronze_links`, their `silver_` equivalents, and `gold_movie_stats`, `gold_movie_genres`, `gold_interactions`, and `gold_quality_report` after a successful run. Movie statistics use all timestamps and are intended for descriptive analysis only. Define train/validation/test cutoffs before fitting or evaluating recommendation models.

## Limitations

Source records are processed as a batch, and the pipeline downloads the full archive on each run. Outputs are overwritten sequentially; failed or concurrent runs can expose incomplete or mixed versions. The saved job permits one run at a time, but manually started notebooks require coordination. Quality counts combine rejected and duplicate records rather than reporting each reason. Team access is scoped to the project schema and includes modifying tables and volume files. Dataset exports remain subject to GroupLens research and redistribution conditions, including restrictions on commercial use.

## AI disclosure

OpenAI Codex assisted with drafting the data pipeline and documentation. The team is responsible for reviewing, validating and understanding the submitted work.
