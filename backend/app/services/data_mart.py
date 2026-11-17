"""数据集市服务：DAG 图、按事实键去重的度量明细包与度量核对包。

业务背景：集市事实表关联多个维度成员（例如一笔订单挂到多个销售区域）时，
同一个度量值会出现在每一行。直接对度量列求和会把经营数值重复累计，
因此对外交付的度量明细包必须按事实键去重：一个事实键只保留一份度量，
维度成员则收进同一条明细的「维度桥接说明」里。

去重时事实键与维度桥接必须由同一集市表版本解释，否则会把合法的维度引用
当成重复行删掉。这里的读取入口统一通过 ``_load_scope`` 完成版本收敛。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from app.store import store

MODULE = "data_mart"
REQUIRED_FIELDS = ["表编号", "表名称", "主题域"]
STATUS_ORDER = ["草稿", "已建模", "已发布", "已下线"]
ACTION_RULES = {"提交建模": "已建模", "发布集市": "已发布", "下线集市": "已下线"}
NEGATIVE_ACTIONS = ["下线集市"]

# 度量明细包 / 核对包使用的口径
MEASURE_FIELD = "订单金额"
FACT_KEY_FIELD = "fact_key"
FACT_KEY_LABEL = "订单编号"
DIMENSION_NAME = "销售区域"
DEFAULT_TABLE = "DATA-0003"
DEFAULT_VERSION = "V3"

# 集市表版本边界：事实键与维度桥接关系只在同一个版本内解释。
TABLE_VERSIONS: dict[str, list[str]] = {
    "DATA-0003": ["V2", "V3"],
    "DATA-0002": ["V1"],
}
CURRENT_VERSIONS: dict[str, str] = {
    "DATA-0003": "V3",
    "DATA-0002": "V1",
}

# 集市事实数据（原样保留，便于核对重复累计的差额）。同一事实键挂多个维度成员时，
# 度量字段在每一行都会复制一份，这是现状，不是明细包应该交付的形态。
FACT_ROWS: list[dict[str, Any]] = [
    {"id": 1, "table_no": "DATA-0003", "version": "V3", "fact_key": "ORDER-1001", "客户": "甲公司", DIMENSION_NAME: "华东", MEASURE_FIELD: 1200.0},
    {"id": 2, "table_no": "DATA-0003", "version": "V3", "fact_key": "ORDER-1001", "客户": "甲公司", DIMENSION_NAME: "华南", MEASURE_FIELD: 1200.0},
    {"id": 3, "table_no": "DATA-0003", "version": "V3", "fact_key": "ORDER-1002", "客户": "乙公司", DIMENSION_NAME: "华北", MEASURE_FIELD: 800.0},
    {"id": 4, "table_no": "DATA-0003", "version": "V3", "fact_key": "ORDER-1002", "客户": "乙公司", DIMENSION_NAME: "华南", MEASURE_FIELD: 800.0},
    {"id": 5, "table_no": "DATA-0003", "version": "V3", "fact_key": "ORDER-1003", "客户": "丙公司", DIMENSION_NAME: "华东", MEASURE_FIELD: 500.0},
    {"id": 6, "table_no": "DATA-0003", "version": "V2", "fact_key": "ORDER-1001", "客户": "甲公司", DIMENSION_NAME: "华东", MEASURE_FIELD: 1000.0},
    {"id": 7, "table_no": "DATA-0003", "version": "V2", "fact_key": "ORDER-1002", "客户": "乙公司", DIMENSION_NAME: "华北", MEASURE_FIELD: 700.0},
    {"id": 8, "table_no": "DATA-0003", "version": "V2", "fact_key": "ORDER-1004", "客户": "丁公司", DIMENSION_NAME: "西北", MEASURE_FIELD: 300.0},
    {"id": 9, "table_no": "DATA-0002", "version": "V1", "fact_key": "ORDER-2001", "客户": "戊公司", DIMENSION_NAME: "华东", MEASURE_FIELD: 600.0},
    {"id": 10, "table_no": "DATA-0002", "version": "V1", "fact_key": "ORDER-2001", "客户": "戊公司", DIMENSION_NAME: "华南", MEASURE_FIELD: 600.0},
    {"id": 11, "table_no": "DATA-0002", "version": "V1", "fact_key": "ORDER-2002", "客户": "己公司", DIMENSION_NAME: "华东", MEASURE_FIELD: 400.0},
    {"id": 12, "table_no": "DATA-0002", "version": "V1", "fact_key": "ORDER-2002", "客户": "己公司", DIMENSION_NAME: "华南", MEASURE_FIELD: 400.0},
    {"id": 13, "table_no": "DATA-0002", "version": "V1", "fact_key": "ORDER-2003", "客户": "庚公司", DIMENSION_NAME: "华东", MEASURE_FIELD: 1000.0},
    {"id": 14, "table_no": "DATA-0002", "version": "V1", "fact_key": "ORDER-2003", "客户": "庚公司", DIMENSION_NAME: "华南", MEASURE_FIELD: 1000.0},
    {"id": 15, "table_no": "DATA-0002", "version": "V1", "fact_key": "ORDER-2003", "客户": "庚公司", DIMENSION_NAME: "西北", MEASURE_FIELD: 1000.0},
]

# 维度桥接关系：事实键 -> 维度成员。必须与事实数据使用同一版本读取。
BRIDGE_ROWS: list[dict[str, Any]] = [
    {"id": 1, "table_no": "DATA-0003", "version": "V3", "fact_key": "ORDER-1001", "dimension": DIMENSION_NAME, "member": "华东"},
    {"id": 2, "table_no": "DATA-0003", "version": "V3", "fact_key": "ORDER-1001", "dimension": DIMENSION_NAME, "member": "华南"},
    {"id": 3, "table_no": "DATA-0003", "version": "V3", "fact_key": "ORDER-1002", "dimension": DIMENSION_NAME, "member": "华北"},
    {"id": 4, "table_no": "DATA-0003", "version": "V3", "fact_key": "ORDER-1002", "dimension": DIMENSION_NAME, "member": "华南"},
    {"id": 5, "table_no": "DATA-0003", "version": "V3", "fact_key": "ORDER-1003", "dimension": DIMENSION_NAME, "member": "华东"},
    {"id": 6, "table_no": "DATA-0003", "version": "V2", "fact_key": "ORDER-1001", "dimension": DIMENSION_NAME, "member": "西南"},
    {"id": 7, "table_no": "DATA-0003", "version": "V2", "fact_key": "ORDER-1002", "dimension": DIMENSION_NAME, "member": "华北"},
    {"id": 8, "table_no": "DATA-0003", "version": "V2", "fact_key": "ORDER-1004", "dimension": DIMENSION_NAME, "member": "西北"},
    {"id": 9, "table_no": "DATA-0002", "version": "V1", "fact_key": "ORDER-2001", "dimension": DIMENSION_NAME, "member": "华东"},
    {"id": 10, "table_no": "DATA-0002", "version": "V1", "fact_key": "ORDER-2001", "dimension": DIMENSION_NAME, "member": "华南"},
    {"id": 11, "table_no": "DATA-0002", "version": "V1", "fact_key": "ORDER-2002", "dimension": DIMENSION_NAME, "member": "华东"},
    {"id": 12, "table_no": "DATA-0002", "version": "V1", "fact_key": "ORDER-2002", "dimension": DIMENSION_NAME, "member": "华南"},
    {"id": 13, "table_no": "DATA-0002", "version": "V1", "fact_key": "ORDER-2003", "dimension": DIMENSION_NAME, "member": "华东"},
    {"id": 14, "table_no": "DATA-0002", "version": "V1", "fact_key": "ORDER-2003", "dimension": DIMENSION_NAME, "member": "华南"},
    {"id": 15, "table_no": "DATA-0002", "version": "V1", "fact_key": "ORDER-2003", "dimension": DIMENSION_NAME, "member": "西北"},
]

# 集市度量核对包：保存期望口径，用于核对消费出来的度量明细包。
PACKS: list[dict[str, Any]] = [
    {
        "id": 1,
        "pack_no": "PACK-0001",
        "name": "订单主题集市度量核对包",
        "table_no": "DATA-0003",
        "version": "V3",
        "mode": "collapse",
        "expected_rows": 3,
        "expected_measure": 2500.0,
        "status": "待核对",
        "created_at": "2026-11-10",
        "expected_items": [
            {"fact_key": "ORDER-1001", "measure": 1200.0},
            {"fact_key": "ORDER-1002", "measure": 800.0},
            {"fact_key": "ORDER-1003", "measure": 500.0},
        ],
        "checksum": "",
    },
    {
        "id": 2,
        "pack_no": "PACK-0002",
        "name": "订单主题集市度量核对包（历史版本）",
        "table_no": "DATA-0003",
        "version": "V2",
        "mode": "collapse",
        "expected_rows": 3,
        "expected_measure": 2200.0,
        "status": "待核对",
        "created_at": "2026-11-09",
        "expected_items": [
            {"fact_key": "ORDER-1001", "measure": 1200.0},
            {"fact_key": "ORDER-1002", "measure": 700.0},
            {"fact_key": "ORDER-1004", "measure": 300.0},
        ],
        "checksum": "",
    },
    {
        "id": 3,
        "pack_no": "PACK-0003",
        "name": "区域主题集市度量核对包",
        "table_no": "DATA-0002",
        "version": "V1",
        "mode": "collapse",
        "expected_rows": 3,
        "expected_measure": 2000.0,
        "status": "待核对",
        "created_at": "2026-11-08",
        "expected_items": [
            {"fact_key": "ORDER-2001", "measure": 600.0},
            {"fact_key": "ORDER-2002", "measure": 400.0},
            {"fact_key": "ORDER-2003", "measure": 1000.0},
        ],
        "checksum": "",
    },
    {
        "id": 4,
        "pack_no": "PACK-0004",
        "name": "订单主题集市度量分摊核对包",
        "table_no": "DATA-0003",
        "version": "V3",
        "mode": "allocate",
        "expected_rows": 5,
        "expected_measure": 2500.0,
        "status": "待核对",
        "created_at": "2026-11-11",
        "expected_items": [
            {"fact_key": "ORDER-1001", "measure": 1200.0},
            {"fact_key": "ORDER-1002", "measure": 800.0},
            {"fact_key": "ORDER-1003", "measure": 500.0},
        ],
        "checksum": "",
    },
]

# 每个核对包只保留一份有效核对结果（重复提交 / 重复核对都覆盖同一份）。
RESULTS: dict[int, dict[str, Any]] = {}
# 上传回执：导入校验失败或成功都留一条可追溯回执。
RECEIPTS: list[dict[str, Any]] = []


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


def _now() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


class MartScopeError(ValueError):
    """集市表版本无法收敛时抛出，由路由层转成可读提示。"""


class DataMartService:
    # ---------- DAG 拓扑（保持原有行为） ----------
    def get_graph(self) -> dict[str, Any]:
        rows = store.rows(MODULE)
        nodes: list[dict] = []
        edges: list[dict] = []
        for i, row in enumerate(rows):
            nodes.append({"id": int(row.get("id", i+1)), "name": str(row.get(list(row.keys())[1] if len(row)>1 else "name", "")), "status": str(row.get("status", STATUS_ORDER[0])),
                          **{k: row[k] for k in row if k not in ("id","status","pending","abnormal")}})
        for i in range(len(nodes)-1):
            edges.append({"from": nodes[i]["id"], "to": nodes[i+1]["id"], "label": "依赖"})
        return {"nodes": nodes, "edges": edges}

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id",0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"集市表 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据集市可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"集市表已{action}"

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]

    # ---------- 版本口径 ----------
    def versions(self) -> list[dict[str, Any]]:
        return [
            {"table_no": table_no, "current": CURRENT_VERSIONS.get(table_no, versions[-1]),
             "versions": list(versions)}
            for table_no, versions in TABLE_VERSIONS.items()
        ]

    def _normalize_version(self, table_no: str, version: str | None) -> str:
        versions = TABLE_VERSIONS.get(table_no)
        if not versions:
            raise MartScopeError(f"集市表 {table_no} 不存在，无法解释事实键与维度桥接")
        candidate = str(version or CURRENT_VERSIONS.get(table_no, versions[-1])).strip().upper()
        if candidate not in versions:
            raise MartScopeError(f"集市表 {table_no} 不存在版本 {candidate}，可用版本：{'、'.join(versions)}")
        return candidate

    def _load_scope(self, table_no: str, version: str) -> tuple[list[dict], list[dict]]:
        """一次读取收敛到同一集市表版本的事实与桥接，避免跨版本误删维度引用。"""
        facts = [row for row in FACT_ROWS if row["table_no"] == table_no and row["version"] == version]
        bridges = [row for row in BRIDGE_ROWS if row["table_no"] == table_no and row["version"] == version]
        return facts, bridges

    @staticmethod
    def _ordered_members(rows: list[dict]) -> list[str]:
        members: list[str] = []
        for row in rows:
            member = str(row.get("member", ""))
            if member and member not in members:
                members.append(member)
        return members

    def _baseline(self, facts: list[dict]) -> dict[str, Any]:
        """以同一版本的事实数据为基准：一个事实键只计一次度量。"""
        keys: list[str] = []
        measures: dict[str, float] = {}
        for row in facts:
            key = str(row.get(FACT_KEY_FIELD, ""))
            if key not in measures:
                keys.append(key)
                measures[key] = _as_float(row.get(MEASURE_FIELD))
        return {
            "fact_key_count": len(keys),
            "fact_row_count": len(facts),
            "measure_total": round(sum(measures.values()), 2),
            "raw_measure_total": round(sum(_as_float(row.get(MEASURE_FIELD)) for row in facts), 2),
            "fact_keys": keys,
        }

    # ---------- 度量明细包 ----------
    def build_detail(
        self,
        table_no: str = DEFAULT_TABLE,
        version: str | None = None,
        pack_id: int | None = None,
        bridge_version: str | None = None,
        mode: str = "collapse",
    ) -> dict[str, Any]:
        """读取既有事实与桥接后按事实键去重，整理成度量明细包。

        ``mode="collapse"``：维度成员收进一条事实记录，度量只计一次（默认交付形态）；
        ``mode="allocate"``：按维度组合分摊度量，同一事实键拆成多行，合计仍等于事实度量。
        两种口径得到的是不同的明细包，核对包必须显式声明自己按哪种口径消费。
        """
        if mode not in ("collapse", "allocate"):
            raise MartScopeError(f"未知的明细口径 {mode}，只支持 collapse（收进一条）或 allocate（按维度分摊）")
        pack = self.get_pack(pack_id) if pack_id is not None else None
        if pack is None and pack_id is not None:
            raise MartScopeError(f"集市度量核对包 {pack_id} 不存在")
        if pack is not None:
            table_no = str(pack["table_no"])
            version = str(pack["version"])
            mode = str(pack.get("mode") or mode)
        resolved_version = self._normalize_version(table_no, version)
        bridge_scope = self._normalize_version(table_no, bridge_version or resolved_version)

        facts, bridges = self._load_scope(table_no, resolved_version)
        # 桥接关系必须与事实键同版本解释；传入不同版本时只做显式冲突提示，不跨版本取数。
        bridge_rows = bridges if bridge_scope == resolved_version else self._load_scope(table_no, bridge_scope)[1]

        baseline = self._baseline(facts)
        grouped: dict[str, list[dict]] = {}
        for row in sorted(facts, key=lambda item: int(item.get("id", 0))):
            key = str(row.get(FACT_KEY_FIELD, ""))
            grouped.setdefault(key, []).append(row)

        items: list[dict[str, Any]] = []
        conflicts: list[dict[str, Any]] = []

        if bridge_scope != resolved_version:
            conflicts.append({
                "scope": "包级",
                "code": "VERSION_SCOPE_MISMATCH",
                "message": f"事实键按版本 {resolved_version} 读取，维度桥接却按版本 {bridge_scope} 读取；已保留同版本桥接，避免误删合法维度引用",
                "fact_key": None,
            })

        for fact_key, rows in grouped.items():
            variants: list[float] = []
            for row in rows:
                value = _as_float(row.get(MEASURE_FIELD))
                if not any(abs(value - seen) < 1e-9 for seen in variants):
                    variants.append(value)
            measure = variants[0] if variants else 0.0
            if len(variants) > 1:
                conflicts.append({
                    "scope": "行级",
                    "code": "MEASURE_DIVERGED",
                    "message": f"事实键 {fact_key} 在同一集市表版本 {resolved_version} 内出现多个度量值：{'、'.join(f'{v:.2f}' for v in variants)}；明细包只保留首个度量，需人工确认分摊口径",
                    "fact_key": fact_key,
                })

            bridge_for_key = [row for row in bridge_rows if str(row.get("fact_key", "")) == fact_key]
            members = self._ordered_members(bridge_for_key)
            fact_members = self._ordered_members([
                {"member": row.get(DIMENSION_NAME, "")} for row in rows
            ])
            if not bridge_rows:
                conflicts.append({
                    "scope": "包级",
                    "code": "BRIDGE_VERSION_MISSING",
                    "message": f"集市表 {table_no} 版本 {resolved_version} 没有维度桥接记录，无法解释事实键的维度成员",
                    "fact_key": None,
                })
            missing_members = [member for member in fact_members if member not in members]
            if bridge_for_key and missing_members:
                conflicts.append({
                    "scope": "行级",
                    "code": "DIMENSION_REFERENCE_MISSING",
                    "message": f"事实键 {fact_key} 的维度成员 {'、'.join(missing_members)} 未出现在版本 {resolved_version} 的维度桥接中；去重时保留事实引用并提示补齐桥接",
                    "fact_key": fact_key,
                })

            if mode == "allocate" and members:
                # 按维度组合分摊：每份 = 事实度量 / 成员数，末位吸收分位余数，合计严格等于事实度量。
                share = round(measure / len(members), 2)
                for position, member in enumerate(members):
                    value = share if position < len(members) - 1 else round(measure - share * (len(members) - 1), 2)
                    note = (
                        f"事实键 {fact_key} 按集市表 {table_no} 版本 {resolved_version} 的维度桥接"
                        f"分摊到{DIMENSION_NAME}：{member}；该份度量 {value:.2f}，与其余"
                        f"{len(members) - 1} 个成员合计 {measure:.2f}"
                    )
                    items.append({
                        "id": f"{fact_key}::{member}",
                        "seq": 0,
                        "fact_key": fact_key,
                        FACT_KEY_LABEL: fact_key,
                        "member": member,
                        "table_no": table_no,
                        "version": resolved_version,
                        "measure_field": MEASURE_FIELD,
                        "measure": value,
                        MEASURE_FIELD: value,
                        "measure_variants": [round(item, 2) for item in variants],
                        "dimension_members": [member],
                        DIMENSION_NAME: member,
                        "bridge_note": note,
                        "维度桥接说明": note,
                        "source_rows": len(rows),
                        "allocated_from": round(measure, 2),
                        "duplicate_measure": 0.0,
                    })
                continue

            member_text = "、".join(members) if members else "无（桥接缺失）"
            bridge_note = (
                f"事实键 {fact_key} 按集市表 {table_no} 版本 {resolved_version} 的维度桥接关联"
                f"{DIMENSION_NAME}：{member_text}；度量 {measure:.2f} 只计一次"
            )
            items.append({
                "id": fact_key or f"ROW-{len(items) + 1}",
                "seq": 0,
                "fact_key": fact_key,
                FACT_KEY_LABEL: fact_key,
                "member": member_text,
                "table_no": table_no,
                "version": resolved_version,
                "measure_field": MEASURE_FIELD,
                "measure": round(measure, 2),
                MEASURE_FIELD: round(measure, 2),
                "measure_variants": [round(value, 2) for value in variants],
                "dimension_members": members,
                DIMENSION_NAME: member_text,
                "bridge_note": bridge_note,
                "维度桥接说明": bridge_note,
                "source_rows": len(rows),
                "allocated_from": None,
                "duplicate_measure": round(measure * (len(rows) - 1), 2),
            })

        for index, item in enumerate(items, start=1):
            item["seq"] = index

        row_count = len(items)
        measure_total = round(sum(item["measure"] for item in items), 2)
        duplicate_total = round(baseline["raw_measure_total"] - measure_total, 2)
        consistent = (
            abs(measure_total - baseline["measure_total"]) < 0.005
            and (mode == "allocate" or row_count == baseline["fact_key_count"])
        )
        return {
            "table_no": table_no,
            "version": resolved_version,
            "fact_version": resolved_version,
            "bridge_version": bridge_scope,
            "mode": mode,
            "measure_field": MEASURE_FIELD,
            "items": items,
            "total": row_count,
            "page": 1,
            "size": row_count or 20,
            "row_count": row_count,
            "fact_key_count": baseline["fact_key_count"],
            "measure_total": measure_total,
            "fact_row_count": baseline["fact_row_count"],
            "fact_measure_total": baseline["raw_measure_total"],
            "duplicate_measure_total": duplicate_total,
            "baseline": baseline,
            "consistent": consistent,
            "conflicts": conflicts,
            "pack_id": pack_id,
            "bridge_note": (
                f"度量明细包按事实键{'分摊度量' if mode == 'allocate' else '去重'}："
                f"{baseline['fact_row_count']} 行事实收敛为 {row_count} 条明细，"
                f"度量合计 {measure_total:.2f}；被避免的重复累计 {duplicate_total:.2f}"
            ),
        }

    def paginate_detail(self, detail: dict[str, Any], page: int = 1, size: int = 20) -> dict[str, Any]:
        """列表读取按稳定顺序切片，行号与明细内容不错位。"""
        items = list(detail.get("items", []))
        total = len(items)
        safe_page = max(1, int(page))
        safe_size = max(1, min(int(size), 200))
        start = (safe_page - 1) * safe_size
        result = dict(detail)
        result.update({
            "items": items[start:start + safe_size],
            "total": total,
            "page": safe_page,
            "size": safe_size,
        })
        return result

    # ---------- 核对包读取 ----------
    def list_packs(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = sorted((dict(row) for row in PACKS), key=lambda row: int(row.get("id", 0)))
        if keyword:
            needle = keyword.casefold()
            rows = [
                row for row in rows
                if needle in str(row.get("pack_no", "")).casefold()
                or needle in str(row.get("name", "")).casefold()
            ]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        safe_page = max(1, int(page))
        safe_size = max(1, min(int(size), 200))
        start = (safe_page - 1) * safe_size
        return rows[start:start + safe_size], total

    def get_pack(self, pack_id: int) -> dict[str, Any] | None:
        for row in PACKS:
            if int(row.get("id", 0)) == int(pack_id):
                return row
        return None

    def get_pack_detail(self, pack_id: int) -> dict[str, Any] | None:
        """详情只返回当前核对包的数据，不夹带列表里上一条的残留。"""
        pack = self.get_pack(pack_id)
        if pack is None:
            return None
        evaluation = self._evaluate(pack)
        detail = evaluation["detail"]
        result = dict(pack)
        result.update({
            "expected_items": list(pack.get("expected_items", [])),
            "actual_items": detail["items"],
            "mode": detail["mode"],
            "row_count": detail["row_count"],
            "fact_key_count": detail["fact_key_count"],
            "measure_total": detail["measure_total"],
            "fact_row_count": detail["fact_row_count"],
            "fact_measure_total": detail["fact_measure_total"],
            "duplicate_measure_total": detail["duplicate_measure_total"],
            "conflicts": evaluation["conflicts"],
            "consistent": evaluation["ok"],
            "result": RESULTS.get(int(pack["id"])),
            "bridge_note": detail["bridge_note"],
        })
        return result

    # ---------- 核对包编排 ----------
    @staticmethod
    def _scope_conflicts(pack: dict[str, Any], detail: dict[str, Any]) -> list[dict[str, Any]]:
        conflicts = list(detail["conflicts"])
        expected_rows = _as_int(pack.get("expected_rows"), detail["row_count"])
        expected_measure = round(_as_float(pack.get("expected_measure"), detail["measure_total"]), 2)
        if expected_rows != detail["row_count"]:
            conflicts.append({
                "scope": "包级",
                "code": "ROW_COUNT_MISMATCH",
                "message": f"核对包期望行数 {expected_rows}，明细包实际行数 {detail['row_count']}",
                "fact_key": None,
            })
        if abs(expected_measure - detail["measure_total"]) >= 0.005:
            conflicts.append({
                "scope": "包级",
                "code": "MEASURE_TOTAL_MISMATCH",
                "message": f"核对包期望度量合计 {expected_measure:.2f}，明细包实际度量合计 {detail['measure_total']:.2f}",
                "fact_key": None,
            })
        actual_by_key: dict[str, float] = {}
        for item in detail["items"]:
            key = str(item["fact_key"])
            actual_by_key[key] = round(actual_by_key.get(key, 0.0) + _as_float(item["measure"]), 2)
        for expected in pack.get("expected_items", []):
            key = str(expected.get("fact_key", ""))
            expected_value = round(_as_float(expected.get("measure")), 2)
            actual_value = actual_by_key.get(key)
            if actual_value is None:
                conflicts.append({
                    "scope": "行级",
                    "code": "FACT_KEY_MISSING",
                    "message": f"核对包期望的事实键 {key} 在明细包中不存在",
                    "fact_key": key,
                })
            elif abs(expected_value - actual_value) >= 0.005:
                conflicts.append({
                    "scope": "行级",
                    "code": "FACT_MEASURE_MISMATCH",
                    "message": f"事实键 {key} 期望度量 {expected_value:.2f}，明细包实际 {actual_value:.2f}",
                    "fact_key": key,
                })
        return conflicts

    def _evaluate(self, pack: dict[str, Any]) -> dict[str, Any]:
        detail = self.build_detail(table_no=str(pack["table_no"]), version=str(pack["version"]), mode=str(pack.get("mode") or "collapse"))
        conflicts = self._scope_conflicts(pack, detail)
        ok = not conflicts
        result = {
            "pack_id": int(pack["id"]),
            "pack_no": pack["pack_no"],
            "table_no": pack["table_no"],
            "version": pack["version"],
            "mode": detail["mode"],
            "expected_rows": _as_int(pack.get("expected_rows")),
            "actual_rows": detail["row_count"],
            "expected_measure": round(_as_float(pack.get("expected_measure")), 2),
            "actual_measure": detail["measure_total"],
            "row_count_match": _as_int(pack.get("expected_rows")) == detail["row_count"],
            "measure_match": abs(round(_as_float(pack.get("expected_measure")), 2) - detail["measure_total"]) < 0.005,
            "status": "一致" if ok else "存在冲突",
            "ok": ok,
            "conflicts": conflicts,
            "bridge_note": detail["bridge_note"],
            "detail": detail,
        }
        return result

    def reconcile_pack(self, pack_id: int) -> tuple[dict[str, Any] | None, str]:
        """编排核对包，验证消费结果；同一核对包只保留一份有效结果。"""
        pack = self.get_pack(pack_id)
        if pack is None:
            return None, f"集市度量核对包 {pack_id} 不存在"
        result = self._evaluate(pack)
        result.pop("detail", None)
        result["checked_at"] = _now()
        RESULTS[int(pack["id"])] = result
        pack["status"] = result["status"]
        pack["checked_at"] = result["checked_at"]
        return result, f"集市度量核对包已核对：{result['status']}"

    def reconcile_latest(self) -> tuple[dict[str, Any] | None, str]:
        if not PACKS:
            return None, "当前没有可核对的集市度量核对包"
        latest = sorted(PACKS, key=lambda row: int(row.get("id", 0)))[-1]
        return self.reconcile_pack(int(latest["id"]))

    def latest_result(self) -> dict[str, Any] | None:
        if not RESULTS:
            return None
        return RESULTS[max(RESULTS)]

    def list_receipts(self) -> list[dict[str, Any]]:
        return list(reversed(RECEIPTS))

    # ---------- 导入校验 / 上传回执 ----------
    @staticmethod
    def _payload_values(payload: dict[str, Any] | None) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        values = payload.get("values")
        if isinstance(values, dict):
            return dict(values)
        return dict(payload)

    @staticmethod
    def _pick(values: dict[str, Any], *keys: str) -> Any:
        for key in keys:
            if key in values and values[key] not in (None, ""):
                return values[key]
        return None

    def _checksum(self, items: list[dict[str, Any]]) -> str:
        canonical = json.dumps(items, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.md5(canonical.encode("utf-8")).hexdigest()

    def _receipt(self, pack_no: str, ok: bool, reasons: list[str], action: str = "导入校验") -> dict[str, Any]:
        receipt = {
            "receipt_no": f"RCPT-{len(RECEIPTS) + 1:04d}",
            "pack_no": pack_no,
            "action": action,
            "status": "上传成功" if ok else "上传失败",
            "reasons": reasons,
            "at": _now(),
        }
        RECEIPTS.append(receipt)
        return receipt

    def import_pack(self, payload: dict[str, Any] | None) -> dict[str, Any]:
        """导入集市度量核对包：结构校验、版本校验与幂等写入。"""
        values = self._payload_values(payload)
        pack_no = str(self._pick(values, "pack_no", "packNo", "包编号") or "").strip()
        name = str(self._pick(values, "name", "pack_name", "包名称") or "").strip()
        table_no = str(self._pick(values, "table_no", "tableNo", "表编号", "table") or "").strip()
        version = str(self._pick(values, "version", "pack_version", "版本") or "").strip().upper()
        mode = str(self._pick(values, "mode", "明细口径") or "collapse").strip().lower()
        mode = {"收进一条": "collapse", "collapse": "collapse", "按维度分摊": "allocate", "allocate": "allocate"}.get(mode, mode)

        reasons: list[str] = []
        if not pack_no:
            reasons.append("缺少核对包编号")
        if not table_no:
            reasons.append("缺少集市表编号")
        if not version:
            reasons.append("缺少核对包版本")
        if mode not in ("collapse", "allocate"):
            reasons.append(f"未知的明细口径 {mode}，只支持 collapse（收进一条）或 allocate（按维度分摊）")

        raw_items = self._pick(values, "items", "rows", "明细行")
        items: list[dict[str, Any]] = []
        if raw_items is not None:
            if not isinstance(raw_items, list):
                reasons.append("明细行必须是数组")
            else:
                seen: set[str] = set()
                for raw in raw_items:
                    if not isinstance(raw, dict):
                        reasons.append("明细行格式不正确")
                        continue
                    fact_key = str(self._pick(raw, "fact_key", "factKey", "事实键", "订单编号") or "").strip()
                    measure = self._pick(raw, "measure", "amount", "度量值", "订单金额")
                    member = str(self._pick(raw, "member", "dimension_member", "维度成员", "销售区域") or "").strip()
                    if not fact_key:
                        reasons.append("明细行缺少事实键")
                        continue
                    if measure is None or _as_float(measure, -1) < 0:
                        reasons.append(f"事实键 {fact_key} 的度量值不合法")
                        continue
                    if fact_key in seen and mode == "collapse":
                        reasons.append(f"事实键 {fact_key} 重复；收进一条（collapse）口径的明细包必须按事实键去重")
                        continue
                    if fact_key in seen and mode == "allocate" and not member:
                        # 分摊口径允许同一事实键出现多次，但每次都要标清摊到哪个维度成员。
                        reasons.append(f"事实键 {fact_key} 在分摊口径下重复出现，必须标注维度成员")
                        continue
                    seen.add(fact_key)
                    items.append({"fact_key": fact_key, "measure": round(_as_float(measure), 2)})

        if table_no and table_no not in TABLE_VERSIONS:
            reasons.append(f"集市表 {table_no} 不存在")
        elif table_no and version and version not in TABLE_VERSIONS[table_no]:
            reasons.append(f"集市表 {table_no} 不存在版本 {version}")
        else:
            facts, _ = self._load_scope(table_no, version) if table_no and version else ([], [])
            if table_no and version and not facts:
                reasons.append(f"集市表 {table_no} 版本 {version} 没有事实数据，无法核对")

        if reasons:
            receipt = self._receipt(pack_no or "未提供", False, reasons)
            return {"ok": False, "message": "核对包导入校验失败，已生成上传失败回执", "entry": None,
                    "duplicate": False, "receipt": receipt, "conflicts": [
                        {"scope": "包级", "code": "IMPORT_VALIDATION_FAILED", "message": reason, "fact_key": None}
                        for reason in reasons
                    ]}

        assert table_no and version
        facts, _ = self._load_scope(table_no, version)
        baseline = self._baseline(facts)
        expected_rows = self._pick(values, "expected_rows", "row_count", "期望行数", "明细行数")
        expected_measure = self._pick(values, "expected_measure", "measure_total", "期望度量合计", "度量合计")
        expected_rows = _as_int(expected_rows if expected_rows is not None else (len(items) or baseline["fact_key_count"]))
        expected_measure = round(_as_float(
            expected_measure if expected_measure is not None else (sum(item["measure"] for item in items) or baseline["measure_total"])
        ), 2)
        if expected_rows < 0 or expected_measure < 0:
            reasons.append("期望行数与期望度量合计必须为非负数")
        if items and abs(round(sum(item["measure"] for item in items), 2) - expected_measure) >= 0.005:
            reasons.append("明细行度量合计与期望度量合计不一致")

        checksum = str(self._pick(values, "checksum", "校验和") or "").strip()
        computed = self._checksum(items)
        if checksum and checksum != computed:
            reasons.append(f"校验和不一致：提交 {checksum}，按明细行计算 {computed}")

        if reasons:
            receipt = self._receipt(pack_no, False, reasons)
            return {"ok": False, "message": "核对包导入校验失败，已生成上传失败回执", "entry": None,
                    "duplicate": False, "receipt": receipt, "conflicts": [
                        {"scope": "包级", "code": "IMPORT_VALIDATION_FAILED", "message": reason, "fact_key": None}
                        for reason in reasons
                    ]}

        existing = next((row for row in PACKS if row["pack_no"] == pack_no and row["version"] == version), None)
        if existing is not None:
            same = (
                existing["table_no"] == table_no
                and str(existing.get("mode") or "collapse") == mode
                and _as_int(existing.get("expected_rows")) == expected_rows
                and abs(_as_float(existing.get("expected_measure")) - expected_measure) < 0.005
                and (not items or existing.get("expected_items") == items)
            )
            if same:
                receipt = self._receipt(pack_no, True, ["核对包已存在，重复提交已忽略，只保留一份有效结果"], "导入校验")
                return {"ok": True, "message": "核对包已存在，重复提交已忽略，未重复写入", "entry": dict(existing),
                        "duplicate": True, "receipt": receipt, "conflicts": []}
            conflict = {
                "scope": "包级",
                "code": "PACK_CONFLICT",
                "message": f"核对包 {pack_no}（版本 {version}）已存在且内容不一致，需先确认处理范围",
                "fact_key": None,
            }
            receipt = self._receipt(pack_no, False, [conflict["message"]], "导入校验")
            return {"ok": False, "message": conflict["message"], "entry": dict(existing), "duplicate": False,
                    "receipt": receipt, "conflicts": [conflict]}

        pack = {
            "id": max((int(row.get("id", 0)) for row in PACKS), default=0) + 1,
            "pack_no": pack_no,
            "name": name or pack_no,
            "table_no": table_no,
            "version": version,
            "mode": mode,
            "expected_rows": expected_rows,
            "expected_measure": expected_measure,
            "status": "待核对",
            "created_at": datetime.now().date().isoformat(),
            "expected_items": items or [
                {"fact_key": item["fact_key"], "measure": item["measure"]}
                for item in self.build_detail(table_no=table_no, version=version, mode=mode)["items"]
            ],
            "checksum": computed,
        }
        PACKS.append(pack)
        receipt = self._receipt(pack_no, True, [], "导入校验")
        return {"ok": True, "message": "集市度量核对包已导入", "entry": dict(pack),
                "duplicate": False, "receipt": receipt, "conflicts": []}

    def upload_pack(self, payload: dict[str, Any] | None) -> dict[str, Any]:
        result = self.import_pack(payload)
        result["receipt"]["action"] = "上传"
        result["message"] = (
            "核对包上传成功" if result["ok"] else "核对包上传失败，已生成上传失败回执"
        )
        return result

    # ---------- 导出 ----------
    def detail_csv_rows(self, detail: dict[str, Any]) -> list[list[Any]]:
        rows: list[list[Any]] = [[
            "表编号", "版本", "事实键", "度量字段", "度量值", "维度成员",
            "维度桥接说明", "来源行数", "重复累计度量",
        ]]
        for item in detail["items"]:
            rows.append([
                detail["table_no"], detail["version"], item["fact_key"], detail["measure_field"],
                item["measure"], "、".join(item["dimension_members"]), item["维度桥接说明"],
                item["source_rows"], item["duplicate_measure"],
            ])
        return rows

    def packs_csv_rows(self) -> list[list[Any]]:
        rows: list[list[Any]] = [[
            "核对包编号", "核对包名称", "表编号", "版本", "明细口径", "期望行数", "实际行数",
            "期望度量合计", "实际度量合计", "状态", "维度桥接说明",
        ]]
        for pack in sorted(PACKS, key=lambda row: int(row.get("id", 0))):
            evaluation = self._evaluate(pack)
            rows.append([
                pack["pack_no"], pack.get("name", ""), pack["table_no"], pack["version"],
                evaluation["mode"],
                pack.get("expected_rows", ""), evaluation["actual_rows"],
                pack.get("expected_measure", ""), evaluation["actual_measure"],
                evaluation["status"], evaluation["bridge_note"],
            ])
        return rows
