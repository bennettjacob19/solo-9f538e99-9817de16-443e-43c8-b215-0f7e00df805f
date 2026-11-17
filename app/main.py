"""FastAPI 历史查询网页：源目录成组修改、提交点查询、捕获进度。

初始化未完成（phase != STREAMING）时，所有历史查询接口返回 503，
不对外开放尚未完整的历史；源目录修改走源库自身事务，不受此限制。
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Annotated, Literal, Union

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field, field_validator

from .capture import CaptureWorker
from .config import settings
from .source import SourceDB, SourceError
from .storage import HistoryStore, lsn_to_num

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("web")

SHA256_RE = r"^[0-9a-fA-F]{64}$"


def _check_sha(v: str) -> str:
    import re

    if not re.match(SHA256_RE, v):
        raise ValueError("sha256 必须是 64 位十六进制")
    return v


class _Columns(BaseModel):
    """增/改共用的四列固定字段，严格拒绝额外列。"""

    artifact_no: int = Field(gt=0)
    service_name: str = Field(min_length=1, max_length=200)
    version: str = Field(min_length=1, max_length=100)
    sha256: str

    @field_validator("sha256")
    @classmethod
    def _sha(cls, v: str) -> str:
        return _check_sha(v)


class InsertOp(_Columns):
    op: Literal["insert"]


class UpdateOp(_Columns):
    op: Literal["update"]


class DeleteOp(BaseModel):
    op: Literal["delete"]
    artifact_no: int = Field(gt=0)


AnyOp = Annotated[
    Union[InsertOp, UpdateOp, DeleteOp], Field(discriminator="op")
]


class BatchRequest(BaseModel):
    operations: list[AnyOp] = Field(min_length=1)


store = HistoryStore(settings.sqlite_path)
source = SourceDB(settings)
worker = CaptureWorker(store, source, settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    worker.start()
    try:
        yield
    finally:
        worker.stop()
        store.close()


app = FastAPI(title="制品目录历史查询", lifespan=lifespan)


def _require_ready() -> None:
    if not store.is_ready():
        progress = store.get_progress()
        raise HTTPException(
            status_code=503,
            detail={
                "message": "捕获初始化尚未完成，历史暂不开放",
                "phase": progress.get("phase"),
                "last_error": progress.get("last_error"),
            },
        )


# --------------------------------------------------------------------- 页面
@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(
        os.path.join(os.path.dirname(__file__), "static", "index.html")
    )


# --------------------------------------------------------------------- 进度
@app.get("/api/progress")
def api_progress() -> dict:
    return store.get_progress()


# --------------------------------------------------------------- 源目录读写
@app.get("/api/source/artifacts")
def api_source_list() -> dict:
    return {"artifacts": source.list_artifacts()}


@app.post("/api/source/operations")
def api_batch_write(req: BatchRequest) -> dict:
    """成组修改源目录；整组在一个源事务内提交，失败整体回滚。"""
    payload = [op.model_dump() for op in req.operations]
    try:
        result = source.apply_operations(payload)
    except SourceError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"源库错误: {exc}")
    result["operations"] = len(payload)
    return result


# ---------------------------------------------------------------- 历史查询
@app.get("/api/points")
def api_points() -> dict:
    _require_ready()
    return {"points": store.get_points()}


@app.get("/api/directory")
def api_directory(
    point: str | None = None, seq: int | None = None
) -> JSONResponse:
    """回看某个提交点的整张目录（只含所选事务及此前状态）。

    两种定位方式，必须且只能给一种：
    - seq=<提交点序号>：精确定位，每笔事务独立回看（新链接推荐）；
    - point=<LSN>：兼容旧链接。该 LSN 只对应一笔提交点时直接回看；
      对应多笔事务（旧版本曾把积压事务记成同一 LSN）时返回 409 与
      候选列表，由调用方按 seq 逐笔选择。旧事务的 LSN 是当时的
      记录值，不伪造真实源 LSN。
    """
    _require_ready()
    if (point is None) == (seq is None):
        raise HTTPException(
            status_code=400,
            detail="point 与 seq 必须且只能提供一个",
        )
    if seq is not None:
        selected = store.get_point_by_seq(seq)
        if selected is None:
            raise HTTPException(
                status_code=404,
                detail=f"序号 {seq} 不是已捕获的提交点，请先在 /api/points 中选择",
            )
    else:
        try:
            lsn_num = lsn_to_num(point)
        except (ValueError, IndexError, AttributeError):
            raise HTTPException(
                status_code=400, detail="point 必须是 LSN，形如 0/16B3748"
            )
        candidates = store.get_points_by_lsn_num(lsn_num)
        if not candidates:
            raise HTTPException(
                status_code=404,
                detail="该 LSN 不是已捕获的提交点，请先在 /api/points 中选择",
            )
        if len(candidates) > 1:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": (
                        "该 LSN 对应多笔事务（旧版本把同批积压事务记成了同一"
                        "提交点），无法唯一确定，请按序号逐笔选择"
                    ),
                    "candidates": [
                        {
                            "seq": c["seq"],
                            "kind": c["kind"],
                            "xid": c["xid"],
                            "commit_ts": c["commit_ts"],
                            "change_count": c["change_count"],
                            "lsn_kind": c["lsn_kind"],
                        }
                        for c in candidates
                    ],
                    "hint": "使用 /api/directory?seq=<序号> 查看对应事务的目录",
                },
            )
        selected = candidates[0]
    return JSONResponse(
        {
            "point": selected["lsn"],
            "point_lsn_num": selected["lsn_num"],
            "seq": selected["seq"],
            "point_info": selected,
            "artifacts": store.get_directory_at(selected["seq"]),
        }
    )


@app.get("/api/history/{artifact_no}")
def api_artifact_history(artifact_no: int) -> dict:
    """单个制品编号的完整变更链；删除后重用编号，旧历史也全部保留。"""
    _require_ready()
    return {
        "artifact_no": artifact_no,
        "history": store.get_history_of(artifact_no),
    }
