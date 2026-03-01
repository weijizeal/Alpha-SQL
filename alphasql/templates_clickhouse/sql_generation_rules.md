# ClickHouse SQL生成规则

## 零、核心禁止规则（必须遵守！）

### 1. 禁止在 WHERE 子句中直接使用窗口函数
```sql
-- ❌ 错误：窗口函数在 WHERE 中
SELECT * FROM t WHERE ROW_NUMBER() OVER (ORDER BY id) = 1

-- ✅ 正确：先计算窗口函数，再过滤
SELECT * FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY id) as rn FROM t) sub WHERE rn = 1
```

### 2. 禁止在聚合函数中嵌套窗口函数
```sql
-- ❌ 错误：聚合函数中嵌套窗口函数
SELECT SUM(AVG(close) OVER (...)) FROM t

-- ✅ 正确：分别计算，再用 JOIN 或子查询连接
```

### 3. lagInFrame/leadInFrame 也不能在 WHERE 中直接使用
```sql
-- ❌ 错误：lagInFrame 在 WHERE 中
WHERE lagInFrame(close, 1) OVER (PARTITION BY secucode ORDER BY date) > close

-- ✅ 正确：先在子查询中计算 lagInFrame，再在外层过滤
SELECT secucode FROM (
    SELECT secucode, date, close,
           lagInFrame(close, 1) OVER (PARTITION BY secucode ORDER BY date) AS prev_close
    FROM hq_history_agg_view
) sub WHERE prev_close > close
```

---

## 一、时间窗口规则

### 1. 时间窗口3要素

**第一要素：时间窗口大小**
- 时间范围 ≥ 窗口长度
- 实际查询时向前多取几天：`lookback_days = n + 4`
- 例如：取5日均线，需要至少5天数据 → 查询时取 n+4 天确保有足够数据

**第二要素：排序获取行号**
```sql
ROW_NUMBER() OVER (PARTITION BY hq_history_agg_view.secucode ORDER BY hq_history_agg_view.date DESC) AS rn
```
- 按股票代码分区（PARTITION BY）
- 按日期降序排序（ORDER BY date DESC）
- rn=1 就是最新一天的数据

**第三要素：取数条数**
- 只要1个数据 → 取 rn = 1
- 需要2个数据（如比较昨日和今日）→ 取 rn = 1 和 rn = 2
- 需要N个数据 → 取 rn = 1, 2, 3, ..., N

### 2. 窗口函数不能直接在WHERE中使用

```sql
-- ❌ 错误：窗口函数不能直接在WHERE中使用
WHERE ROW_NUMBER() OVER (...) = 1

-- ✅ 正确：先查询到临时表（CTE）中，再在外层过滤
WITH ma_with_rn AS (
    SELECT secucode, date, ma_value,
           ROW_NUMBER() OVER (PARTITION BY secucode ORDER BY date DESC) AS rn
    FROM hq_history_agg_view
)
SELECT secucode FROM ma_with_rn WHERE rn = 1
```

#### 3. lagInFrame/leadInFrame 也不能在 WHERE 中直接使用

```sql
-- ❌ 错误：lagInFrame 在 WHERE 中
WHERE lagInFrame(close, 1) OVER (PARTITION BY secucode ORDER BY date) > close

-- ✅ 正确：先在子查询中计算 lagInFrame，再在外层过滤
SELECT secucode FROM (
    SELECT secucode, date, close,
           lagInFrame(close, 1) OVER (PARTITION BY secucode ORDER BY date) AS prev_close
    FROM hq_history_agg_view
) sub WHERE prev_close > close
```

#### 完整示例：站上5日线
-- 1. 时间窗口：需要至少5天数据（实际取n+4天确保有最新一天）
-- 2. 计算MA5：AVG(close) OVER (ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
-- 3. 获取最新一天：ROW_NUMBER() OVER (PARTITION BY secucode ORDER BY date DESC)，取 rn=1
-- 4. 不能直接在WHERE用窗口函数，需要先CTE再过滤

WITH ma_cte AS (
    SELECT secucode, date, ma_5, rn
    FROM (
        SELECT secucode, date,
               AVG(close) OVER (PARTITION BY secucode ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS ma_5,
               ROW_NUMBER() OVER (PARTITION BY secucode ORDER BY date DESC) AS rn
        FROM hq_history_agg_view
        WHERE date BETWEEN '2026-01-01' AND '2026-02-28'
    )
    WHERE rn = 1  -- 只取最新一天
),
close_cte AS (
    SELECT secucode, close
    FROM hq_history_agg_view
    WHERE date = '2026-02-28'  -- 最新交易日
)
SELECT DISTINCT close_cte.secucode 
FROM close_cte 
JOIN ma_cte ON close_cte.secucode = ma_cte.secucode
WHERE close_cte.close > ma_cte.ma_5  -- 收盘价 > 5日均线

---

## 二、多条件连接规则

### 1. INTERSECT（交集）- 同时满足多个筛选条件

**使用场景**：当问题表示"同时满足"、"并且"、"和/加"时

| 问题示例 | 条件 | 连接方式 |
|---------|------|---------|
| 创业板+2024年上市 | 创业板 AND 2024年上市 | INTERSECT |
| 北斗导航+军工 | 同时属于两个概念 | INTERSECT |
| 机器人+绿电 | 同时满足两个概念 | INTERSECT |
| 涨停+低位 | 同时满足两个条件 | INTERSECT |

```sql
-- 筛选 创业板 AND 2024年上市
SELECT secucode FROM 创业板 INTERSECT SELECT secucode FROM 2024年上市
```

### 2. UNION（并集）- 满足任一条件

**使用场景**：当问题表示"或者"、"和/及"（表示"任一"）时

| 问题示例 | 条件 | 连接方式 |
|---------|------|---------|
| 科创板+创业板 | 科创板 OR 创业板 | UNION |
| 688开头+30开头 | 688开头 OR 30开头 | UNION |

```sql
-- 筛选 科创板 OR 创业板
SELECT secucode FROM 科创板 UNION SELECT secucode FROM 创业板
```

### 3. JOIN（连接）- 从不同表获取关联数据

**使用场景**：需要从多个表获取数据并关联时

| 情况 | 示例 | 连接方式 |
|-----|------|---------|
| 关联股票基本信息 | 行情表 JOIN 基本面表 | INNER JOIN |
| 获取股东户数 | 股票表 LEFT JOIN 股东户数表 | LEFT JOIN |
| 计算MA并比较 | 收盘价CTE JOIN MA值CTE | INNER JOIN |

```sql
-- 收盘价 JOIN MA值
SELECT close_cte.secucode
FROM close_cte
JOIN ma_cte ON close_cte.secucode = ma_cte.secucode
WHERE close_cte.close > ma_cte.ma_5
```

---

## 三、时间窗口规则（包含聚合/窗口函数时使用）

### 1. 时间窗口使用条件

**触发条件**：当问题需要分析"多天数据"时使用，如：
- 1. **包含聚合相关函数**：需要计算N日均线（AVG）、N日累计（SUM）、N日最大最小等
- 2. **需要比较今天和昨天/历史**：两天数据对比、计算变化率、是否翻倍等
- 3. **需要取最新数据**：表中有多个日期，只需最新一天的数据

### 2. 具体应用场景（从dev.json提取）

| 场景 | 问题示例 | 窗口函数 | 处理方式 |
|-----|---------|---------|---------|
| **N日均线** | 站上5日线的股票、5日均线上穿10日均线 | AVG() OVER | 计算MA5、MA10，比较大小 |
| **N日累计** | 近3日资金流入、近5日资金流入 | SUM() OVER | 累计N天资金流入，排序取前N |
| **今日vs昨日** | 今日主力资金流入是上一日翻倍 | lagInFrame() | 今日值 - lagInFrame(昨日值)，计算比值 |
| **多日比较** | 3日主力资金流入的股票 | lagInFrame() + 差值 | 今日 - 3日前 = 3日流入 |
| **取最新一天** | 收盘价等于最低价、涨停股 | ROW_NUMBER() | 按日期降序，取rn=1 |
| **连续N天** | 连续涨停、连续资金流入 | ROW_NUMBER() + 差值 | 排序后计算日期间隔 |

### 3. 关键词与处理方式对照表

| 关键词 | 含义 | 窗口函数 | 示例 |
|-------|------|---------|------|
| N日均线、MA5、MA10 | 移动平均线 | AVG() OVER | AVG(close) OVER (ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) |
| 近N天、近N日 | 最近N天累计 | SUM() OVER | SUM(zhu) OVER (ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW) |
| 上一日、昨日、比上一日 | 与前一天比较 | lagInFrame() | zhu - lagInFrame(zhu, 1) OVER (ORDER BY date) |
| 近3日、近5日 | N日流入 | lagInFrame()差值 | zhu - lagInFrame(zhu, N) |
| 翻倍、一倍 | 倍数比较 | lagInFrame()比值 | 今日/昨日 >= 2 |
| 最多、前N | 排名 | ORDER BY | ORDER BY zhu DESC LIMIT N |

### 4. 窗口函数不能在WHERE中直接使用

```sql
-- ❌ 错误
WHERE ROW_NUMBER() OVER (...) = 1

-- ✅ 正确：先CTE再过滤
WITH temp AS (
    SELECT *, ROW_NUMBER() OVER (...) AS rn FROM table
)
SELECT * FROM temp WHERE rn = 1
```

### 5. Few-shot 完整示例

#### 示例1：站上5日线的股票
- **关键词**：5日均线 → 需要AVG计算
- **SQL结构**：
```sql
-- 1. 计算5日均线
AVG(close) OVER (PARTITION BY secucode ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS ma_5
-- 2. 取最新一天（rn=1）
ROW_NUMBER() OVER (PARTITION BY secucode ORDER BY date DESC) AS rn
-- 3. 比较收盘价 > MA5
```

#### 示例2：近3日资金流入最多的个股
- **关键词**：近3日 → 需要SUM累计
- **SQL结构**：
```sql
-- 1. 计算3日累计流入
zhu - lagInFrame(zhu, 3) OVER (PARTITION BY secucode ORDER BY date) AS zhu_3d
-- 2. 筛选今日数据（rn=1）
-- 3. 按3日流入排序取前N
```

#### 示例3：今日主力资金流入是上一日翻倍的股票
- **关键词**：今日、上一日、翻倍 → 需要lagInFrame比较
- **SQL结构**：
```sql
-- 1. 获取今日和昨日主力资金
lagInFrame(zhu, 1) OVER (PARTITION BY secucode ORDER BY date) AS zhu_lag1
-- 2. 计算比值
zhu / lagInFrame(zhu, 1) >= 2  -- 翻倍
-- 3. 筛选rn=1（最新一天）
```

#### 示例4：5日均线上穿10日均线
- **关键词**：5日均线、10日均线、上穿 → 需要两个MA比较
- **SQL结构**：
```sql
-- 1. 计算MA5和MA10
AVG(close) OVER (ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS ma_5
AVG(close) OVER (ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma_10
-- 2. 昨日MA5 <= MA10 AND 今日MA5 > MA10（手机上穿）
-- 3. 需要比较连续两天的MA值变化
```

#### 示例5：收盘价等于最低价（取最新一天）
- **关键词**：当日 → 需要取最新一天
- **SQL结构**：
```sql
-- 1. 窗口函数生成行号
ROW_NUMBER() OVER (PARTITION BY secucode ORDER BY date DESC) AS rn
-- 2. 过滤rn=1取最新一天
WHERE rn = 1 AND close = low
```

### 6. 多条件连接关键词

**触发条件**：问题中涉及多条件组合时使用

| 关键词 | 含义 | SQL连接 |
|-------|------|---------|
| 同时、和、加、且、并且 | 多个条件都满足 | INTERSECT |
| 或者、或、及 | 任一条件满足 | UNION |
| 关联、从...获取 | 不同表数据关联 | JOIN |

**Few-shot 示例**：

**示例1：问题"创业板+2024年上市"**
- 关键词：+（和） → 多个条件都满足
- 处理：INTERSECT
```sql
SELECT secucode FROM 创业板 INTERSECT SELECT secucode FROM 2024年上市
```

**示例2：问题"科创板+创业板"**
- 关键词：+（和）但实际是"或" → 任一条件满足
- 处理：UNION
```sql
SELECT secucode FROM 科创板 UNION SELECT secucode FROM 创业板
```

**示例3：问题"获取股东户数"**
- 关键词：获取 → 需要关联
- 处理：JOIN
```sql
SELECT hq_basic_view.secucode, f10_shareholder_nums_view.total_people_num
FROM hq_basic_view
LEFT JOIN f10_shareholder_nums_view ON hq_basic_view.secucode = f10_shareholder_nums_view.secucode
```
