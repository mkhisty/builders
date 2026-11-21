#!/usr/bin/env python3
"""Compare native rolling attribution with exhaustive click lookups for known sessions."""
import csv
from collections import defaultdict
from pathlib import Path
import random
import subprocess
import tempfile
import sys

import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
WEEK = 604800000
rng = random.Random(4814)
channels = {'c_a': 'search', 'c_b': 'social', 'c_c': 'email'}
rows = []
expected = defaultdict(lambda: [0, 0, 0, 0])
base = 1800000000000
for person in ('a', 'b', 'c'):
    # Known, disjoint sessions avoid duplicating the native sessionization logic.
    sessions = [(base + i * 3600000, rng.choice((0, 0, 250))) for i in range(520)]
    clicks = [(f'r{i:04}', base + rng.randrange(-8 * 86400000, 10 * 86400000), rng.choice(tuple(channels)))
              for i in range(1500)]
    for i in (0, 24, 240):
        start = sessions[i][0]
        clicks += [(f'boundary{i}', start - WEEK, 'c_a'),
                   (f'expired{i}', start - WEEK - 1, 'c_b'),
                   (f'after{i}', start + 1, 'c_c'),
                   (f'A{i}', start, 'c_b'), (f'z{i}', start, 'c_a')]
    for cid, ts, campaign in clicks:
        rows.append((person, ts, 1, cid, campaign, 0))
    for i, (start, revenue) in enumerate(sessions):
        rows += [(person, start, 2, f'e{i}a', 'books', 0),
                 (person, start + 1000, 2, f'e{i}b', 'books', revenue)]
        eligible = [(cid, ts, campaign) for cid, ts, campaign in clicks if start - WEEK <= ts <= start]
        winner = min(eligible, key=lambda c: (-c[1], c[0]))[2] if eligible else 'organic'
        day = start // 86400000
        credited = expected[winner, day]
        credited[0] += 1
        credited[1] += revenue > 0
        credited[2] += revenue
        for campaign in {c[2] for c in eligible}:
            expected[campaign, day][3] += 1
# A click-only person must not produce a session or leak attribution into another person.
rows.append(('aa', base, 1, 'only', 'c_c', 0))
rows.sort(key=lambda r: r[:4])
with tempfile.TemporaryDirectory() as folder:
    folder = Path(folder)
    with (folder / 'sorted.csv').open('w', newline='') as out:
        writer = csv.writer(out, lineterminator='\n')
        writer.writerow(['person', 'ts', 'kind', 'id', 'value', 'revenue'])
        writer.writerows(rows)
    with (folder / 'campaigns.csv').open('w', newline='') as out:
        writer = csv.writer(out, lineterminator='\n')
        writer.writerow(['campaign_id', 'channel'])
        writer.writerows(channels.items())
    subprocess.run([str(ROOT / 'pipeline/aggregate'), str(folder / 'sorted.csv'),
                    str(folder / 'campaigns.csv'), str(folder)], check=True)
    def check_output():
        actual = {}
        with (folder / 'campaign_daily.csv').open(newline='') as out:
            for row in csv.DictReader(out):
                assert row['channel'] == channels.get(row['campaign_id'], 'organic')
                key = row['campaign_id'], int(row['day'])
                assert key not in actual
                actual[key] = [int(row[c]) for c in ('sessions', 'purchasing_sessions', 'revenue_cents', 'assisted_sessions')]
        assert actual == dict(expected), 'Rolling attribution differs from exhaustive lookups'

    check_output()
    sys.path.insert(0, str(ROOT / 'pipeline'))
    from native import aggregate_batches

    names = list(dict.fromkeys(['organic'] + [row[4] for row in rows]))
    codes = {name: code for code, name in enumerate(names)}
    with (folder / 'values.csv').open('w', newline='') as out:
        writer = csv.writer(out, lineterminator='\n')
        writer.writerow(['code', 'value'])
        writer.writerows(enumerate(names))
    for text_type in (pa.string(), pa.large_string()):
        schema = pa.schema([('person', text_type), ('ts', pa.int64()), ('kind', pa.int32()),
                            ('id', text_type), ('code', pa.int32()), ('revenue', pa.int64())])
        columns = list(zip(*[(person, ts, kind, cid, codes[value], revenue)
                             for person, ts, kind, cid, value, revenue in rows]))
        table = pa.Table.from_arrays([pa.array(values, type=field.type)
                                     for values, field in zip(columns, schema)], schema=schema)
        # Sliced buffers and tiny batches exercise offsets and native batch lifetimes.
        reader = pa.RecordBatchReader.from_batches(schema, table.to_batches(max_chunksize=17))
        aggregate_batches(reader, folder / 'values.csv', folder / 'campaigns.csv', folder)
        check_output()
    bad = table.set_column(2, 'kind', table.column('kind').cast(pa.int64()))
    try:
        aggregate_batches(bad.to_reader(), folder / 'values.csv', folder / 'campaigns.csv', folder)
    except RuntimeError as error:
        assert 'Unexpected Arrow column type' in str(error)
    else:
        raise AssertionError('Native Arrow interface accepted an invalid schema')
    aggregate_batches(table.to_reader(), folder / 'values.csv', folder / 'campaigns.csv', folder)
    check_output()
print('CSV and Arrow attribution match exhaustive lookups for 1,560 sessions; batch offsets and schema errors pass')
