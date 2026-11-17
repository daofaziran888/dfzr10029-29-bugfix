"""数据集市接口：DAG 拓扑、按事实键去重的度量明细包与集市度量核对包。

路由声明顺序有讲究：``/export`` 与 ``/reconciliation-packs`` 这类静态路径必须排在
``/{entry_id}`` 之前，否则会被当成记录编号吞掉。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.data_mart import DataMartService, MartScopeError

router = APIRouter(prefix="/api/data_mart", tags=["数据集市"])
service = DataMartService()
LIST_FIELDS = ["表编号", "表名称", "主题域", "事实粒度", "关联维度", "度量字段", "刷新周期", "表状态"]
STATUSES = ["草稿", "已建模", "已发布", "已下线"]
PACK_STATUSES = ["待核对", "一致", "存在冲突"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


def excel_safe(value: Any) -> Any:
    """只做公式注入防护，交给 csv.writer 统一处理引号与转义。"""
    if isinstance(value, str) and value[:1] in ("=", "+", "-", "@"):
        return "'" + value
    return value


def write_csv(header: list[Any], rows: list[list[Any]], filename: str) -> StreamingResponse:
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(header)
    for row in rows:
        writer.writerow([excel_safe(value) for value in row])
    return StreamingResponse(
        iter(["﻿" + buf.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'})


def detail_payload(
    table_no: str = Query(default="", description="集市表编号，默认取 DATA-0003"),
    version: str | None = Query(default=None, description="集市表版本；事实键与维度桥接按同一版本解释"),
    bridge_version: str | None = Query(default=None, description="显式指定维度桥接版本，默认跟随事实版本"),
    pack_id: int | None = Query(default=None, description="按核对包口径生成明细包"),
    mode: str = Query(default="collapse", description="collapse=维度成员收进一条；allocate=按维度组合分摊度量"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> dict[str, Any]:
    try:
        detail = service.build_detail(
            table_no=table_no or "DATA-0003",
            version=version,
            pack_id=pack_id,
            bridge_version=bridge_version,
            mode=mode,
        )
    except MartScopeError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    return service.paginate_detail(detail, page=page, size=size)


# ---------- 度量明细包：固定路径在前 ----------
@router.get("/versions")
def list_versions() -> dict[str, Any]:
    """列出集市表可用版本，说明事实键与维度桥接的版本边界。"""
    return {"items": service.versions()}


@router.get("/measure-detail")
def get_measure_detail(
    table_no: str = Query(default="", description="集市表编号，默认取 DATA-0003"),
    version: str | None = Query(default=None),
    bridge_version: str | None = Query(default=None),
    pack_id: int | None = Query(default=None),
    mode: str = Query(default="collapse", description="collapse=维度成员收进一条；allocate=按维度组合分摊度量"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> dict[str, Any]:
    """读取既有事实数据与关联维度，按事实键去重后返回度量明细包。"""
    return detail_payload(table_no, version, bridge_version, pack_id, mode, page, size)


@router.get("/measure-detail/export")
def export_measure_detail(
    table_no: str = Query(default=""),
    version: str | None = Query(default=None),
    bridge_version: str | None = Query(default=None),
    pack_id: int | None = Query(default=None),
    mode: str = Query(default="collapse", description="collapse=维度成员收进一条；allocate=按维度组合分摊度量"),
) -> StreamingResponse:
    """下载带维度桥接说明的度量明细包，用于和事实表版本逐键核对。"""
    try:
        detail = service.build_detail(
            table_no=table_no or "DATA-0003", version=version, pack_id=pack_id,
            bridge_version=bridge_version, mode=mode,
        )
    except MartScopeError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    table = service.detail_csv_rows(detail)
    return write_csv(table[0], table[1:], "data_mart_measure_detail.csv")


# ---------- 核对包：静态路径在前，{pack_id} 在后 ----------
@router.get("/reconciliation-packs")
def list_packs(
    keyword: str | None = Query(default=None, description="按核对包编号或名称检索"),
    status: str | None = Query(default=None, description="待核对、一致、存在冲突"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """列表读取核对包；分页按稳定顺序切片，不与上一条详情混用。"""
    items, total = service.list_packs(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/reconciliation-packs/export")
def export_packs() -> StreamingResponse:
    """导出核对包及其消费结果，附带维度桥接说明。"""
    rows = service.packs_csv_rows()
    return write_csv(rows[0], rows[1:], "data_mart_reconciliation_packs.csv")


@router.get("/reconciliation-packs/receipts")
def list_receipts() -> dict[str, Any]:
    """上传回执列表：导入校验失败也会留下可追溯记录。"""
    return {"items": service.list_receipts()}


@router.get("/reconciliation-packs/latest-result")
def latest_result() -> dict[str, Any]:
    """读取最近一次核对编排的有效结果（重复提交只保留一份）。"""
    result = service.latest_result()
    if result is None:
        raise HTTPException(status_code=404, detail="暂无集市度量核对结果")
    return result


@router.get("/reconciliation-packs/{pack_id}")
def get_pack(pack_id: int) -> dict:
    """核对包详情；只返回当前核对包，不保留列表里其它记录的残留。"""
    pack = service.get_pack_detail(pack_id)
    if pack is None:
        raise HTTPException(status_code=404, detail=f"集市度量核对包 {pack_id} 不存在")
    return pack


@router.post("/reconciliation-packs/import")
def import_pack(payload: EntryPayload) -> dict[str, Any]:
    """导入校验集市度量核对包：结构、版本与幂等写入一并校验。"""
    return service.import_pack({"values": payload.values, "remark": payload.remark})


@router.post("/reconciliation-packs/upload")
def upload_pack(payload: EntryPayload) -> dict[str, Any]:
    """上传核对包；失败时返回上传失败回执而不是静默丢弃。"""
    return service.upload_pack({"values": payload.values, "remark": payload.remark})


@router.post("/reconciliation-packs/{pack_id}/reconcile")
def reconcile_pack(pack_id: int) -> dict[str, Any]:
    """编排核对包验证消费结果；同一核对包重复核对只写入一份有效结果。"""
    result, message = service.reconcile_pack(pack_id)
    if result is None:
        return {"ok": False, "message": message, "entry": None}
    return {"ok": True, "message": message, "entry": result}


@router.post("/reconciliation")
def reconcile_latest() -> dict[str, Any]:
    """集市核对接口：编排最新一个集市度量核对包并验证消费结果。"""
    result, message = service.reconcile_latest()
    if result is None:
        return {"ok": False, "message": message, "entry": None}
    return {"ok": True, "message": message, "entry": result}


# ---------- DAG 拓扑（既有行为） ----------
@router.get("")
def get_graph() -> dict[str, Any]:
    """返回节点与边组成的 DAG 图数据。"""
    return service.get_graph()


@router.get("/export")
def export_entries() -> StreamingResponse:
    nodes, edges = service.raw_data()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(["type", "id", "name", "from", "to", "label", "status"])
    for node in nodes:
        writer.writerow(["node", node.get("id",""), csv_cell(node.get("name","")), "", "", "", node.get("status","")])
    for edge in edges:
        writer.writerow(["edge", "", "", edge.get("from",""), edge.get("to",""), edge.get("label",""), ""])
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=data_mart.csv"})


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"集市表 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="集市表已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
