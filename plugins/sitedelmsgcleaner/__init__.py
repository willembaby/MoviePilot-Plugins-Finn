# -*- coding: utf-8 -*-
"""
站点删种消息自动清理（SiteDelMsgCleaner）

检测 MoviePilot 内置「站点数据刷新解析未读站内信」时推送的「站点」消息
（EventType.NoticeMessage 广播，mtype=SiteMessage），一旦发现「种子被删除」类站内信，
自动删除对应的下载器种子、数据源文件与转移记录（transferhistory），并保留回收站审计记录。

安全约束：
- 只有能明确提取出种子名、且能在下载器命中同名记录时才删除；无法提取或未命中则跳过。
- 数据文件共享保护：候选数据路径若仍被删除范围外的其它种子引用，则降级为仅删任务、不删数据。
- 回收站审计记录（name/hash/site/delete_time）按保留天数滚动清理（0=永久保留）。
- 已处理消息按 source/title+text 去重，防重复触发。
"""

import logging
import re
from datetime import datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.db.transferhistory_oper import TransferHistoryOper
from app.helper.downloader import DownloaderHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.message import Notification
from app.schemas.types import EventType, NotificationType
from app.utils.event import EventManager

eventmanager = EventManager()

# 归一化：小写 + 去所有非字母数字（保留中文）
_re_nonalnum = re.compile(r"[^0-9a-z\u4e00-\u9fff]")


def _norm(name: str) -> str:
    return _re_nonalnum.sub("", (name or "").lower())


def _field(t, *keys):
    """兼容 TR 对象属性 / QB 字典的字段取值"""
    for key in keys:
        try:
            v = t.get(key) if isinstance(t, dict) else getattr(t, key, None)
        except Exception:
            v = None
        if v:
            return v
    return None


def _field_hash(t):
    return _field(t, "hash_string", "hashString", "hash", "id")


def _field_path(t):
    return _field(t, "content_path", "contentPath", "save_path", "savePath",
                  "download_dir", "downloadDir")


# 站点消息正文里「种子'…'」的引号内完整名
_re_quote_name = re.compile(r"['\"]([^'\"\n]{8,})['\"]")
# 无引号时的「种子：xxx被删」
_re_seed_name = re.compile(r"种子[：: ]?\s*([^\s，。;'\",\n]{6,}?)\s*(?:被删除|被删|已被|被管理员)")
# 标题里的站点名
_re_site = re.compile(r"(?:【)?站点\s*([^\s】。]+)")


class SiteDelMsgCleaner(_PluginBase):
    plugin_name = "站点删种消息自动清理"
    plugin_desc = "检测站点删除种子类站内信，自动删除对应种子、数据源文件与转移记录"
    plugin_icon = "https://raw.githubusercontent.com/willembaby/MoviePilot-Plugins-Finn/main/icon.png"
    plugin_version = "1.0.0"
    plugin_author = "Finn"
    author_url = ""
    plugin_config_prefix = "sitedelmsgcleaner_"
    plugin_order = 30
    auth_level = 1

    def __init__(self):
        super().__init__()
        self._enabled = False
        self._auto_delete = True
        self._delete_data = True
        self._delete_transfer = True
        self._match_keywords = ""
        self._retention_days = 30
        self._onlyonce = False
        self._scheduler = None
        self._processed = set()

    def init_plugin(self, config: dict = None):
        self._enabled = bool((config or {}).get("enabled"))
        self._auto_delete = bool((config or {}).get("auto_delete", True))
        self._delete_data = bool((config or {}).get("delete_data", True))
        self._delete_transfer = bool((config or {}).get("delete_transfer", True))
        self._match_keywords = str((config or {}).get("match_keywords", "") or "")
        try:
            self._retention_days = int((config or {}).get("retention_days", 30))
        except (TypeError, ValueError):
            self._retention_days = 30
        self._onlyonce = bool((config or {}).get("onlyonce", False))

        if not self._enabled:
            return

        if self._scheduler:
            try:
                self._scheduler.shutdown(wait=False)
            except Exception:
                pass
        self._scheduler = BackgroundScheduler()
        self._scheduler.add_job(self._maintain_recycle, CronTrigger.from_crontab("0 3 * * *"),
                                id="sitedelmsgcleaner_recycle", max_instances=1, coalesce=True)
        if self._onlyonce:
            self._scheduler.add_job(self._maintain_recycle, "date",
                                    run_date=datetime.now() + timedelta(seconds=1),
                                    id="sitedelmsgcleaner_once")
            self.update_config({"onlyonce": False})
        self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    # ------------------------------------------------------------------
    # 事件入口（接住内置站点消息推送）
    # ------------------------------------------------------------------
    @eventmanager.register(EventType.NoticeMessage)
    def on_notice(self, event) -> None:
        if not self.get_state():
            return
        try:
            self._on_notice(event)
        except Exception as e:
            logger.error(f"{self.plugin_name} 处理通知事件出错：{e}")

    def _on_notice(self, event) -> None:
        data = event.event_data or {}
        if not isinstance(data, dict) or not data:
            return
        if data.get("channel"):            # 带渠道的外发消息不回
            return
        msg_type = data.get("type") or data.get("mtype")
        if msg_type != NotificationType.SiteMessage:
            return
        title = str(data.get("title") or "")
        text = str(data.get("text") or "")
        msg_id = str(data.get("source") or data.get("message_id") or "")
        dedup_key = msg_id or f"{title}|{text[:200]}"
        if dedup_key in self._processed:
            return
        self._processed.add(dedup_key)
        # 关键词过滤
        keywords = [k.strip() for k in self._match_keywords.split(",") if k.strip()]
        if not keywords:
            logger.info(f"{self.plugin_name} 未配置触发关键词，跳过：{title}")
            return
        body = f"{title}\n{text}"
        body_low = body.lower()
        if not any(k.lower() in body_low for k in keywords):
            return
        names = self._extract_names(body)
        if not names:
            logger.info(f"{self.plugin_name} 命中关键词但无法提取种子名，跳过：{text[:80]}")
            return
        site_name = self._site_from_title(title)
        if self._auto_delete:
            self._execute_delete(names, site_name=site_name, source=dedup_key)
        else:
            self.post_message(Notification(
                mtype=NotificationType.Plugin,
                title=f"【{self.plugin_name}】检测到站点删种消息",
                text=f"站点：{site_name or '未知'}\n种子：\n" + "\n".join(names) +
                     "\n（当前为「仅记录」模式，未执行删除）",
                link=data.get("link") or "",
            ))

    @staticmethod
    def _extract_names(body: str) -> list:
        names, seen = [], set()
        # 优先引号内完整名
        for m in _re_quote_name.finditer(body):
            cand = m.group(1).strip()
            if len(cand) >= 8 and _norm(cand):
                nk = _norm(cand)
                if nk not in seen:
                    seen.add(nk)
                    names.append(cand)
        if not names:
            for m in _re_seed_name.finditer(body):
                cand = m.group(1).strip()
                if _norm(cand):
                    nk = _norm(cand)
                    if nk not in seen:
                        seen.add(nk)
                        names.append(cand)
        return names

    @staticmethod
    def _site_from_title(title: str) -> str:
        m = _re_site.search(title or "")
        return m.group(1) if m else ""

    # ------------------------------------------------------------------
    # 删除执行
    # ------------------------------------------------------------------
    def _execute_delete(self, names, site_name="", source=""):
        services = DownloaderHelper().get_services()
        if not services:
            logger.warning(f"{self.plugin_name} 无可用下载器，跳过删除")
            return

        all_torrents = []  # (svc_name, torrent)
        for svc_name, svc in services.items():
            inst = getattr(svc, "instance", None)
            if not inst:
                continue
            try:
                torrents, err = inst.get_torrents()
            except Exception as e:
                logger.error(f"{self.plugin_name} 获取下载器 {svc_name} 种子出错：{e}")
                continue
            if err or not torrents:
                continue
            for t in torrents:
                all_torrents.append((svc_name, t))

        candidates = []
        for svc_name, t in all_torrents:
            tname = _field(t, "name", "Name")
            if any(self._name_match(n, tname) for n in names):
                candidates.append((svc_name, t))
        if not candidates:
            logger.info(f"{self.plugin_name} 未在下载器中命中种子：{names}")
            return

        # 数据共享保护：候选数据路径是否被范围外种子引用
        cand_paths = {_field_path(t) for _, t in candidates}
        cand_paths = {p for p in cand_paths if p}
        protected = set()
        for svc_name, t in all_torrents:
            if t in [ct for _, ct in candidates]:
                continue
            path = _field_path(t)
            if not path:
                continue
            for cp in cand_paths:
                if cp == path or cp.startswith(path + "/") or path.startswith(cp + "/"):
                    protected.add(cp)
                    break

        stats = {"deleted": 0, "data": 0, "task_only": 0, "transfer": 0, "failed": 0}
        recycle_records = []
        for svc_name, t in candidates:
            tname = _field(t, "name", "Name")
            thash = _field_hash(t)
            path = _field_path(t)
            del_data = bool(self._delete_data) and not (path in protected)
            rec = {
                "deleted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "site": site_name or "",
                "name": tname or "",
                "hash": thash or "",
                "downloader": svc_name or "",
                "delete_data": del_data,
                "source": "",
            }
            recycle_records.append(rec)
            inst = getattr(services.get(svc_name), "instance", None)
            if not inst:
                stats["failed"] += 1
                continue
            try:
                ok = inst.delete_torrents(delete_file=del_data, ids=[thash])
            except Exception as e:
                logger.error(f"{self.plugin_name} 删除种子失败 {tname}: {e}")
                stats["failed"] += 1
                continue
            if not ok:
                stats["failed"] += 1
                continue
            stats["deleted"] += 1
            if del_data:
                stats["data"] += 1
            else:
                stats["task_only"] += 1
            if self._delete_transfer and thash:
                try:
                    hist_list = TransferHistoryOper().list_by_hash(thash)
                    for hist in hist_list:
                        TransferHistoryOper().delete(hist.id)
                        stats["transfer"] += 1
                except Exception as e:
                    logger.error(f"{self.plugin_name} 删除转移记录失败 {tname}: {e}")

        if recycle_records:
            data = self.get_data(self._recycle_key()) or []
            data.extend(recycle_records)
            self.save_data(self._recycle_key(), data)
        self._clean_recycle()
        self._post_result(stats, site_name, len(protected))

    # ---- 名称匹配 ----
    @staticmethod
    def _name_match(msg_name, tor_name) -> bool:
        mn, tn = _norm(msg_name), _norm(tor_name)
        if not mn or not tn:
            return False
        if mn == tn:
            return True
        if len(mn) >= 12 and (mn in tn or tn in mn):
            return True
        return False

    @staticmethod
    def _recycle_key():
        return "recycle"

    def _clean_recycle(self):
        if self._retention_days <= 0:
            return
        data = self.get_data(self._recycle_key()) or []
        cutoff = datetime.now() - timedelta(days=self._retention_days)
        keep, removed = [], 0
        for rec in data:
            try:
                dt = datetime.strptime(rec.get("deleted_at", ""), "%Y-%m-%d %H:%M:%S")
            except (TypeError, ValueError):
                dt = None
            if dt is None or dt >= cutoff:
                keep.append(rec)
            else:
                removed += 1
        if removed:
            self.save_data(self._recycle_key(), keep)
            logger.info(f"{self.plugin_name} 已清理 {removed} 条过期回收站记录")

    def _maintain_recycle(self):
        if not self.get_state():
            return
        self._clean_recycle()

    def _post_result(self, stats, site_name, protected_count):
        lines = [
            f"站点：{site_name or '未知'}",
            f"删除种子：{stats['deleted']}（含数据 {stats['data']} / 仅任务 {stats['task_only']}）",
            f"删除转移记录：{stats['transfer']}",
        ]
        if stats["failed"]:
            lines.append(f"失败：{stats['failed']}")
        if protected_count:
            lines.append(f"⚠ {protected_count} 条数据被跨站引用，已保留文件仅删任务")
        self.post_message(Notification(
            mtype=NotificationType.Plugin,
            title=f"【{self.plugin_name}】已清理站点删种",
            text="\n".join(lines),
        ))

    # ------------------------------------------------------------------
    # 页面 / API
    # ------------------------------------------------------------------
    def get_form(self):
        return [
            {'component': 'VForm', 'content': [
                {'component': 'VRow', 'content': [
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                     'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                     'content': [{'component': 'VSwitch', 'props': {'model': 'auto_delete', 'label': '自动删除',
                                                                     'hint': '检测到删种消息立即执行；关闭则仅记录/通知'}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                     'content': [{'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即运行一次维护'}}]},
                ]},
                {'component': 'VRow', 'content': [
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                     'content': [{'component': 'VSwitch', 'props': {'model': 'delete_data', 'label': '删除数据源文件',
                                                                     'hint': '跨站共享数据时自动降级为仅删任务、保留文件'}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                     'content': [{'component': 'VSwitch', 'props': {'model': 'delete_transfer', 'label': '删除转移记录'}}]},
                ]},
                {'component': 'VRow', 'content': [
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                     'content': [{'component': 'VTextField', 'props': {'model': 'match_keywords', 'label': '触发关键词（逗号分隔）',
                                                                        'placeholder': '种子被删除,被删除,被管理员删除,Nuked,被标记,已删除'}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                     'content': [{'component': 'VTextField', 'props': {'model': 'retention_days', 'label': '回收站记录保留天数（0=永久）',
                                                                        'placeholder': '30'}}]},
                ]},
            ]}
        ], {
            "enabled": False,
            "auto_delete": True,
            "onlyonce": False,
            "delete_data": True,
            "delete_transfer": True,
            "match_keywords": "种子被删除,被删除,被管理员删除,Nuked,被标记,已删除",
            "retention_days": 30,
        }

    def get_page(self):
        recycle = self.get_data(self._recycle_key()) or []
        recycle = sorted(recycle, key=lambda r: r.get("deleted_at", ""), reverse=True)
        page = [
            {'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'density': 'compact',
                                              'class': 'mb-2',
                                              'text': f"回收站审计记录共 {len(recycle)} 条（保留 {self._retention_days} 天）"}},
            {'component': 'VRow', 'props': {'class': 'mb-2'}, 'content': [
                {'component': 'VCol', 'props': {'cols': 12}, 'content': [
                    {'component': 'VBtn', 'props': {'color': 'primary', 'variant': 'tonal', 'size': 'small'},
                     'text': '清空回收站记录',
                     'events': {'click': {'api': 'plugin/SiteDelMsgCleaner/clear_recycle', 'method': 'get',
                                           'params': {'apikey': settings.API_TOKEN}}}},
                ]},
            ]},
        ]
        if not recycle:
            page.append({'component': 'div', 'props': {'class': 'text-center text-grey mt-6'},
                         'text': '暂无清理记录'})
        else:
            for r in recycle[:20]:
                page.append({'component': 'VRow', 'props': {'class': 'mb-1'}, 'content': [
                    {'component': 'VCol', 'props': {'cols': 2},
                     'content': [{'component': 'div', 'text': r.get('deleted_at', '')}]},
                    {'component': 'VCol', 'props': {'cols': 2},
                     'content': [{'component': 'div', 'text': r.get('site', '')}]},
                    {'component': 'VCol', 'props': {'cols': 6},
                     'content': [{'component': 'div', 'text': r.get('name', '')}]},
                    {'component': 'VCol', 'props': {'cols': 1},
                     'content': [{'component': 'div', 'text': r.get('downloader', '')}]},
                    {'component': 'VCol', 'props': {'cols': 1},
                     'content': [{'component': 'div', 'text': '是' if r.get('delete_data') else '否'}]},
                ]})
        return page

    def get_api(self) -> list:
        return [{"path": "/clear_recycle", "endpoint": self.clear_recycle, "methods": ["GET"],
                 "summary": "清空回收站审计记录"}]

    def clear_recycle(self) -> dict:
        self.save_data(self._recycle_key(), [])
        return {"success": True, "message": "已清空回收站记录"}

    def stop_service(self):
        if self._scheduler:
            try:
                self._scheduler.shutdown(wait=False)
            except Exception:
                pass
        self._scheduler = None