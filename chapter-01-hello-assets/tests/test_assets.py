from pathlib import Path
import duckdb
from dagster import materialize

def test_materialize_graph(tmp_path, monkeypatch):
    tmp_data = tmp_path / 'data'
    tmp_data.mkdir(parents=True, exist_ok=True)
    tmp_wh = tmp_data / 'warehouse'
    tmp_wh.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv('PTWR_DATA_DIR', str(tmp_data))

    from pipelines_the_right_way.ch01.assets import raw_iris, iris_clean, iris_summary

    project_raw = Path(__file__).resolve().parents[1] / 'data' / 'raw' / 'iris.csv'
    (tmp_data / 'raw').mkdir(parents=True, exist_ok=True)
    (tmp_data / 'raw' / 'iris.csv').write_text(project_raw.read_text())

    result = materialize([raw_iris, iris_clean, iris_summary])
    assert result.success

    db_path = tmp_wh / 'iris.duckdb'
    assert db_path.exists(), f'Expected DuckDB at {db_path}'

    con = duckdb.connect(str(db_path))
    try:
        count = con.execute('SELECT COUNT(*) FROM iris_summary').fetchone()[0]
    finally:
        con.close()
    assert count > 0
