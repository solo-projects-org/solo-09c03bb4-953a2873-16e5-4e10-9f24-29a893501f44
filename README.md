# 渡轮结算 · Iceberg v2 票号删除核对

用于核对湖仓源快照中，某航次日分区里的票号是否已被 **Iceberg v2 位置删除**
（positional delete）标记。网页接收有效文件清单和对应 Parquet 副本，输入整数
票号，逐条给出原文件、从 0 起算的文件内行号、是否已删除以及命中的删除文件。

## 快速启动

```bash
docker compose up --build
```

打开 <http://localhost:8000>，点 **“载入样例”** 即可运行内置演示。接口健康检查：

```bash
curl http://localhost:8000/healthz
```

停止：

```bash
docker compose down
```

## 网页用法

1. 上传 **有效文件清单**（CSV 或 JSON）。
2. 上传清单中所有 `data` / `position_delete` 文件的 **Parquet 副本**，可多选。
   文件名必须与清单的 `uploaded_filename` 一致。
3. 填写整数票号（逗号、空格、分号或换行分隔），以及票号列名（默认
   `ticket_no`）。
4. 开始核对。

上传文件只保存在一次请求的临时目录中，请求结束后删除；不会按用户文件名
拼接服务器路径。

## 离线路径迁移（对象存储换目录后继续核对）

换了对象存储目录后，数据文件不在本地时，仍可先迁移清单和位置删除副本，
再连同数据副本提交回上面的核对页。入口：<http://localhost:8000/migrate>，
核对页右上角也有 **“离线路径迁移”** 链接。

准备三样上传内容：

1. **现有格式的 Iceberg v2 CSV 清单**（列同上面的 `original_path`、
   `uploaded_filename`、`file_type`、`data_sequence_number`）。
2. **路径对照表 CSV**，两列：`old_path`、`new_path`。
3. **位置删除 Parquet 副本**（清单中的 `position_delete` 文件），文件名与
   `uploaded_filename` 一致。**不需要**上传数据文件副本。

规则：

- 对照表里的每个 `old_path` 必须唯一，且必须精确指向清单里一个 `data`
  文件的 `original_path`；否则按冲突路径列出并不出包。
- 每个原值只替换一次：新旧路径可以交叉（A→B、B→A），替换始终对照迁移前
  的值查表，不会连环替换，也不会出现“替换后又命中另一条规则”。
- 迁移后清单若出现重复路径（例如两个旧数据路径映射到同一个新路径，或新
  路径撞上清单里未迁移的路径），按冲突路径列出并不出包。
- 删除记录的 `file_path` 若**原先并不指向清单内的数据文件**，迁移后却会
  指向迁移后的数据文件，按冲突路径列出并不出包（避免凭空产生有效删除）。
- 清单含 `equality_delete`、任一位置删除副本缺失、Parquet 读不出或缺
  `file_path` / `pos` 列、`pos` 非法，整次中止并说明原因，不出包。
- 输出 ZIP：迁移后的清单（保留列、列顺序、行顺序和除路径外的值）＋清单中
  全部位置删除文件（保留原文件名、列结构、Parquet 字段 ID）。删除行先按
  `file_path`、再按 `pos` 升序排序，除 `file_path` 外的值不变。
- ZIP 里的清单命名为 `manifest.csv`；若下游使用别的名字，解压后改回原清单
  文件名即可（清单内容与副本文件名不受影响）。解压后把清单、迁移后的删除
  副本和原始数据副本一起提交到核查页即可。

内置样例：页面上的 **“载入新旧路径交叉的样例”** 使用 `sample/migrate/`，
其中两个数据文件的 old/new 路径互换（A→B、B→A），用来验证“每个原值只
替换一次”。**“直接下载样例 ZIP”** 可直接拿到结果。

## 清单格式

CSV（推荐）：

```csv
original_path,uploaded_filename,file_type,data_sequence_number
s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05/data/data-0001.parquet,data-0001.parquet,data,7
s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05/delete-pos/delete-pos-0001.parquet,delete-pos-0001.parquet,position_delete,12
```

JSON 也支持，顶层可以是数组，或包含 `files` / `entries` 数组的对象：

```json
[
  {
    "original_path": "s3://.../data/data-0001.parquet",
    "uploaded_filename": "data-0001.parquet",
    "file_type": "data",
    "data_sequence_number": 7
  }
]
```

列名可使用以下别名：

| 含义 | 可用列名 |
| --- | --- |
| 原始路径 | `original_path`, `original_file_path`, `source_path`, `path` |
| 上传文件名 | `uploaded_filename`, `uploaded_file`, `filename` |
| 文件类型 | `file_type`, `content`, `type` |
| 数据序列号 | `data_sequence_number`, `sequence_number`, `seq` |

`file_type` 可选值：

| 值 | 行为 |
| --- | --- |
| `data` / `0` | 数据文件，参与扫描 |
| `position_delete` / `positional_delete` / `1` / `delete` | 位置删除文件，参与匹配 |
| `equality_delete` / `2` | 等值删除，**忽略**；不要求上传 Parquet 副本 |

只处理位置删除。位置删除 Parquet 必须包含 `file_path`（字符串）和 `pos`
（整数）两列，其他列（例如 Iceberg 可选的 `row`）会被忽略。

## 核对规则

- **行组跳读**：对每个数据文件的每个行组读取 `ticket_no` 列的 min/max 统计。
  票号落在统计范围外时跳过该行组；统计缺失（`statistics` 为 `None` 或没有
  min/max）时读取该行组。统计范围包含票号时也读取。
- **原始行号**：行号始终是数据文件中从 0 起算的原始行号 =
  前面所有行组的行数之和 + 行组内原始下标。筛选或只读取某个行组都不会重置、
  压缩或重新编号。
- **位置删除匹配**：删除记录必须同时满足
  1. 删除记录的 `file_path` 精确等于清单中的 `original_path`；
  2. `pos` 精确等于该命中行的从 0 起算的行号；
  3. 删除文件的数据序列号 **不小于** 数据文件的数据序列号。

  不满足序列号条件的候选删除文件会显示为“被规则忽略的删除记录”，但不会
  把票判成已删除。
- **全有或全无**：清单中的任何一个 `data` / `position_delete` 副本缺失，
  或任一文件打不开、行组读取失败，整次核对都会中止并明确列出文件名，不显示
  任何票号的部分结论。未出现在清单中的上传文件会被忽略并给出警告。

## 内置样例

`sample/` 中的演示数据由 `tools/make_sample.py` 生成。位置删除样例带
Iceberg 同名 `file_path` / `pos` / `row` 三列和 Parquet 字段 ID，迁移时
这些结构必须保持不变。

- `data-0001.parquet`：5 个行组，每个 20 行。
  - 行组 0–3 的票号范围分别是 1000–1019、2000–2019、3000–3019、4000–4019。
  - 票号 **4242** 在第 5 个行组（索引 4），全局行号 **99**。
  - 核对 4242 时，前 4 个行组全部被统计范围排除并跳过；读取第 5 个行组后
    仍按原始行号 99 命中 `delete-pos-0001.parquet`。
- `data-0002.parquet`：写入时关闭了列统计，票号 **8888** 在第 2 个行组
  （全局行号 **19**）。因为没有统计，两个行组都会被读取，随后命中
  `delete-pos-0002.parquet`。
- `delete-pos-stale-0001.parquet`：也指向 4242 / 行 99，但数据序列号是 6，
  小于数据文件的 7，因此被忽略。
- 清单中的 `equality_delete` 项没有上传副本，核对仍正常完成，因为它被忽略。

样例默认票号：`4242, 1003, 4218, 8888, 9999`。其中 1003 已删除，4218
存在但未删除，9999 不存在。

## 本地开发与测试

需要 Python 3.11+、Flask、PyArrow。

```bash
python3 -m pip install -r requirements.txt
python3 app.py                     # http://localhost:8000
python3 -m pytest -q               # 62 个测试
```

重新生成样例（会覆盖 `sample/` 下的 Parquet 和清单）：

```bash
python3 tools/make_sample.py
```

## 目录

```
app.py                     Flask 路由、上传处理、票号解析、迁移下载
checker/manifest.py        清单解析与校验
checker/engine.py          PyArrow 行组跳读、行号计算、位置删除匹配
checker/migrate.py         离线路径迁移：对照表校验、清单改写、删除文件重写与 ZIP
checker/errors.py          CheckError（始终带文件名，可带冲突路径）
templates/index.html       核对网页
templates/migrate.html     离线迁移网页
static/style.css           样式
sample/                    内置核对样例（清单 + Parquet）
sample/migrate/            内置迁移样例（交叉路径的清单、对照表、删除副本）
tools/make_sample.py       样例生成脚本
tests/                     pytest 测试
Dockerfile                 容器镜像
docker-compose.yml         启动配置
```

## 已知边界

- 位置删除文件会整体读入内存建立 `(file_path, pos)` 索引；迁移时也会整体
  读入并重写，适用于常见规模。
- 每个票号独立扫描数据文件。一次输入大量票号时耗时随票号数线性增长。
- 路径按字符串精确比较，不解析 `s3://`、`file://` 等 URI 等价关系。
  请保证清单的 `original_path` 与删除记录中的 `file_path` 写法完全一致。
- 迁移会重写删除 Parquet，因此会生成新的 Parquet 文件（压缩为 snappy）；
  列结构、字段 ID 与除 `file_path` 外的值不变，物理字节编码可能变化。
- 迁移只处理位置删除；清单含等值删除时按规则中止，不静默丢弃。
