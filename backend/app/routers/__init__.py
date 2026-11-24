"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

``correction`` 提供固定路径 ``/api/correction/*``，必须排在资源路由
（含 ``/api/{module}``）之前，否则会被动态路径抢先匹配。
"""
from __future__ import annotations

from app.routers import correction, resource

ROUTERS = [correction, resource]
