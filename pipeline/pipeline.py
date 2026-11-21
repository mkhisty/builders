#!/usr/bin/env python3
"""In-memory C++ ingestion, deduplication, sorting and aggregation; Arrow Parquet output."""
import json
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as parquet

from native import run_pipeline

DATA = Path(os.environ.get('DATA_DIR', '/data'))
OUT = Path(os.environ.get('OUTPUT_DIR', '/output'))
SCRATCH = Path(os.environ.get('SCRATCH_DIR', '/scratch'))
NCPU = int(os.environ.get('NCPU', '8'))
SCRATCH.mkdir(parents=True, exist_ok=True)
pa.set_cpu_count(NCPU)
manifest = json.loads((DATA / 'manifest.json').read_text())
start, end = (int(datetime.fromisoformat(manifest[k].replace('Z', '+00:00')).timestamp()) * 1000
              for k in ('window_start', 'window_end'))

USER = pa.schema([('user_id', pa.string()), ('day', pa.date32()), ('sessions', pa.int64()),
                  ('events', pa.int64()), ('revenue_cents', pa.int64()), ('top_category', pa.string())])
CAMPAIGN = pa.schema([('campaign_id', pa.string()), ('channel', pa.string()), ('day', pa.date32()),
                      ('sessions', pa.int64()), ('purchasing_sessions', pa.int64()),
                      ('revenue_cents', pa.int64()), ('assisted_sessions', pa.int64())])


def batches(path, schema):
    types = {field.name: (pa.int32() if field.name == 'day' else field.type) for field in schema}
    options = csv.ConvertOptions(column_types=types, strings_can_be_null=True,
                                 quoted_strings_can_be_null=False, null_values=[''])
    with csv.open_csv(path, read_options=csv.ReadOptions(use_threads=False),
                      convert_options=options) as reader:
        for batch in reader:
            columns = [batch.column(i).cast(field.type) for i, field in enumerate(schema)]
            yield pa.RecordBatch.from_arrays(columns, schema=schema)


with tempfile.TemporaryDirectory(dir=SCRATCH) as folder:
    temp = Path(folder)
    run_pipeline(DATA, temp, start, end, NCPU)
    began = time.perf_counter()
    user_out = OUT / 'user_daily'
    campaign_out = OUT / 'campaign_daily'
    user_out.mkdir(parents=True, exist_ok=True)
    campaign_out.mkdir(parents=True, exist_ok=True)
    jobs = sorted(temp.glob('worker-*'))

    def write_user(work):
        destination = user_out / ('part-' + work.name.removeprefix('worker-') + '.parquet')
        with parquet.ParquetWriter(destination, USER, compression='snappy') as writer:
            for batch in batches(work / 'user_daily.csv', USER):
                writer.write_batch(batch)

    with ThreadPoolExecutor(max_workers=NCPU) as pool:
        list(pool.map(write_user, jobs))
    campaign_tables = [pa.Table.from_batches(list(batches(work / 'campaign_daily.csv', CAMPAIGN)), schema=CAMPAIGN)
                       for work in jobs]
    totals = pa.concat_tables(campaign_tables).group_by(['campaign_id', 'channel', 'day']).aggregate(
        [(name, 'sum') for name in CAMPAIGN.names[3:]])
    totals = totals.rename_columns(CAMPAIGN.names).cast(CAMPAIGN)
    parquet.write_table(totals, campaign_out / 'part-0.parquet', compression='snappy')
    print(f'Arrow Parquet output: {time.perf_counter() - began:.3f} seconds', flush=True)
