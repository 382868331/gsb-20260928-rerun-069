"""固定研究数据集的持久化层。

数据集是对底层“开放/封存特征值”的一次逐样本、逐特征的组装与冻结：

* research_datasets          数据集主表（draft/frozen，冻结后不可变）；
* research_dataset_features  本数据集采用的特征视角（特征版本/修订单）；
* research_dataset_samples   纳入或尝试纳入的样本及其完整性结论；
* research_dataset_cells     每个 (样本 x feature_key) 的冻结值与血缘；
* research_dataset_pending_diffs 更新已冻结数据集时仍未采用的差异。

数据集一旦 frozen，值与血缘原样保存，重开后逐项读取与冻结时一致；
后续补登、迟到资料、历史特征修订都不会自动改写任何已冻结数据集。
"""

from __future__ import annotations

import sqlite3


DATASET_SCHEMA = """
CREATE TABLE IF NOT EXISTS research_datasets (
    dataset_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT,
    status        TEXT NOT NULL CHECK (status IN ('draft', 'frozen')),
    created_at    INTEGER NOT NULL,
    frozen_at     INTEGER,
    parent_id     INTEGER,
    note          TEXT
);

CREATE TABLE IF NOT EXISTS research_dataset_features (
    dataset_id   INTEGER NOT NULL,
    feature_name TEXT NOT NULL,
    version      INTEGER NOT NULL,
    revision_id  INTEGER,
    PRIMARY KEY (dataset_id, feature_name, version, revision_id)
);

CREATE TABLE IF NOT EXISTS research_dataset_samples (
    dataset_id   INTEGER NOT NULL,
    sample_id    TEXT NOT NULL,
    included     INTEGER NOT NULL,
    reason       TEXT,
    PRIMARY KEY (dataset_id, sample_id)
);

CREATE TABLE IF NOT EXISTS research_dataset_cells (
    dataset_id   INTEGER NOT NULL,
    sample_id    TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    version      INTEGER NOT NULL,
    feature_key  TEXT NOT NULL,
    value        TEXT,
    source_kind  TEXT NOT NULL
                 CHECK (source_kind IN ('sealed', 'revision')),
    seal_id      INTEGER,
    revision_id  INTEGER,
    evidence     TEXT NOT NULL,
    PRIMARY KEY (dataset_id, sample_id, feature_name, version, feature_key)
);

CREATE TABLE IF NOT EXISTS research_dataset_pending_diffs (
    dataset_id   INTEGER NOT NULL,
    sample_id    TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    version      INTEGER NOT NULL,
    feature_key  TEXT NOT NULL,
    category     TEXT NOT NULL
                 CHECK (category IN ('sample_scope', 'revision', 'visible_selection')),
    old_value    TEXT,
    new_value    TEXT,
    detail       TEXT NOT NULL,
    PRIMARY KEY (dataset_id, sample_id, feature_name, version, feature_key)
);
"""


def ensure_dataset_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(DATASET_SCHEMA)
    conn.commit()
