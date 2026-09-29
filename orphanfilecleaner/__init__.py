import bisect
import os
import shutil
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.core.config import settings
from app.helper.downloader import DownloaderHelper
from app.log import logger
from app.plugins import _PluginBase


class OrphanFileCleaner(_PluginBase):
    """
    孤儿文件清理插件（v1.0）

    解决痛点：下载器中存在"数据文件还在、但种子任务已不存在"的孤儿文件
    （删除种子时选择不删文件、手工迁移、中断下载清理种子等场景留下的残留），
    占用存储空间。

    工作原理：
    1. 收集全部已连接下载器（TR / qBittorrent）的种子"内容根路径"作为引用集合
    2. 扫描用户配置的数据文件目录，找出未被任何种子引用的文件/目录（孤儿）
    3. 支持逐条或批量删除孤儿：默认移入回收站目录（可恢复），或彻底删除

    安全设计：
    - 默认"移入回收站"模式，可配置为彻底删除
    - 删除仅针对扫描结果中的孤儿路径，不做任何推断删除
    - 支持下载器容器路径映射（如 /downloads=/media）
    - 删除前校验路径必须位于已配置的扫描目录范围内
    """

    # 插件元数据
    plugin_name = "孤儿文件清理"
    plugin_desc = ("扫描数据文件目录，找出「有数据文件但无种子引用」的孤儿文件/目录"
                   "（删种留文件、迁移残留等），按文件夹分类展示、勾选批量删除、"
                   "检测硬链接并列出地址，支持一键移入回收站或彻底删除。")
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/clean.png"
    plugin_version = "1.6"
    plugin_author = "Finn"
    author_url = "https://github.com"
    plugin_config_prefix = "orphanfilecleaner_"
    plugin_order = 33
    auth_level = 1

    # 配置
    _enabled = False
    _scan_dirs = ""          # 待扫描的数据目录，每行一个
    _paths = ""              # 下载器容器路径映射，如 /downloads=/media;旧=/新
    _delete_mode = "trash"   # trash=移入回收站 / delete=彻底删除
    _trash_dir = ""          # 回收站目录，留空则自动（扫描目录下 .orphan_trash）
    _notify = True
    _notify_title_prefix = "HA通知"
    _hardlink_search = True      # 是否检查硬链接
    _hardlink_search_dirs = ""  # 硬链接额外搜索目录（每行一个），默认在扫描目录内搜索
    _sel: List[str] = []         # 勾选集合（内存缓存，避免每次点击写库产生闪烁延迟）

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled", False)
            self._scan_dirs = str(config.get("scan_dirs") or "")
            self._paths = str(config.get("paths") or "")
            self._delete_mode = str(config.get("delete_mode") or "trash")
            self._trash_dir = str(config.get("trash_dir") or "")
            self._notify = bool(config.get("notify", True))
            self._notify_title_prefix = config.get("notify_title_prefix") or "HA通知"
            self._hardlink_search = bool(config.get("hardlink_search", True))
            self._hardlink_search_dirs = str(config.get("hardlink_search_dirs") or "")
        self._sel = []
        logger.info(f"孤儿文件清理：配置加载完成，enabled={self._enabled}，"
                    f"扫描目录={self._scan_dirs.strip() or '(未配置)'}")

    def get_state(self) -> bool:
        return self._enabled

    def stop_service(self):
        """停止插件服务"""
        pass

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    # ==================== 配置表单 ====================

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        插件配置页面：目录输入 + 删除机制 + 通知
        """
        return [
            {
                'component': 'VForm',
                'content': [
                    # Row1: enabled / delete_mode / notify
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'enabled', 'label': '启用插件'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'delete_mode',
                                            'label': '删除机制',
                                            'items': [
                                                {'title': '移入回收站（可恢复，推荐）', 'value': 'trash'},
                                                {'title': '彻底删除（不可恢复）', 'value': 'delete'}
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'notify', 'label': '发送通知'}
                                    }
                                ]
                            },
                        ]
                    },
                    # Row 2: 扫描目录（整行）
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 12},
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'scan_dirs',
                                            'label': '数据文件目录（每行一个）',
                                            'placeholder': '/media/Transmission\n/media/音乐\n/media/电影/外语电影',
                                            'rows': 4,
                                            'hint': '必填：扫描这些目录中未被任何种子引用的孤儿文件'
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    # Row 3: 路径映射 / 回收站目录
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'paths',
                                            'label': '下载器容器路径映射（可选）',
                                            'placeholder': '/downloads=/media（分号分隔多对）',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'trash_dir',
                                            'label': '回收站目录（可选，默认自动）',
                                            'placeholder': '/media/.orphan_trash',
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    # Row 4: 硬链接检查
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'hardlink_search', 'label': '检查硬链接'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 9},
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'hardlink_search_dirs',
                                            'label': '硬链接额外搜索目录（每行一个，可选）',
                                            'placeholder': '/media\n/media/MediaLib',
                                            'rows': 2,
                                            'hint': '默认在扫描目录内搜索硬链接；媒体库在别处时可在此补充搜索范围'
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    # Row 5: 通知前缀
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 12},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'notify_title_prefix',
                                            'label': '通知前缀',
                                            'placeholder': 'HA通知'
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                ]
            }
        ], {
            "enabled": False,
            "scan_dirs": "",
            "paths": "",
            "delete_mode": "trash",
            "trash_dir": "",
            "notify": True,
            "notify_title_prefix": "HA通知",
            "hardlink_search": True,
            "hardlink_search_dirs": "",
        }

    def get_page(self) -> Optional[List[dict]]:
        """
        插件详情页（重新排版）：
        - 顶部：状态摘要 + 操作按钮（主次分级）
        - 分组：底色横条组头（复选框 + 目录 + 统计 + 组删除）
        - 条目：四列对齐（勾选/路径/大小/操作），已勾选行浅绿背景
        - 底部：最近处理记录
        """
        if not self._enabled:
            return [
                {
                    'component': 'VAlert',
                    'props': {
                        'type': 'warning',
                        'text': '插件未启用，请先到「设置」页启用并填写数据文件目录',
                        'variant': 'tonal'
                    }
                }
            ]

        saved = self.get_data("result") or {}
        items = saved.get("items") or []
        scan_time = saved.get("time", "")
        deleted = self.get_data("deleted") or []
        selected = self._sel
        selected_set = set(selected)
        hl_count = sum(1 for it in items if it.get("has_hardlink"))

        page = []
        # ===== 状态摘要 =====
        page.append({
            'component': 'VAlert',
            'props': {
                'type': 'info',
                'variant': 'tonal',
                'density': 'compact',
                'class': 'mb-2',
                'text': (f"扫描时间：{scan_time or '尚未扫描'}    孤儿 {len(items)} 项    "
                         f"有硬链接 {hl_count} 项    已勾选 {len(selected)} 项")
            }
        })
        # ===== 操作按钮（主次分级） =====
        page.append({
            'component': 'VRow',
            'props': {'class': 'mb-1'},
            'content': [
                {
                    'component': 'VCol',
                    'props': {'cols': 12, 'md': 3},
                    'content': [self._btn("扫描数据目录", 'plugin/OrphanFileCleaner/scan', {}, 'primary', 'elevated')]
                },
                {
                    'component': 'VCol',
                    'props': {'cols': 12, 'md': 3},
                    'content': [self._btn(f"删除选中项（{len(selected)}）", 'plugin/OrphanFileCleaner/delete_selected',
                                          {'confirm': 'yes'}, 'error', 'elevated')]
                },
                {
                    'component': 'VCol',
                    'props': {'cols': 12, 'md': 3},
                    'content': [self._btn("删除全部孤儿", 'plugin/OrphanFileCleaner/delete_all',
                                          {'confirm': 'yes'}, 'error', 'text')]
                },
                {
                    'component': 'VCol',
                    'props': {'cols': 12, 'md': 3},
                    'content': [self._btn("清空结果", 'plugin/OrphanFileCleaner/clear',
                                          {'confirm': 'yes'}, 'secondary', 'text')]
                },
            ]
        })
        # 使用说明（小字）
        page.append({
            'component': 'div',
            'props': {'class': 'text-caption text-grey mb-2'},
            'text': "勾选后点「删除选中项」批量处理；组头复选框一键全选本组；删除方式：移入回收站（可在设置改为彻底删除）"
        })

        if not items:
            page.append({
                'component': 'div',
                'text': '暂无扫描结果，请点击「扫描数据目录」',
                'props': {'class': 'text-center text-grey mt-6'}
            })
        else:
            # ===== 按文件夹分组 =====
            groups = {}
            for it in items:
                d = it.get("dir") or "/"
                groups.setdefault(d, []).append(it)
            for d in sorted(groups.keys()):
                g_items = groups[d]
                sel_in_group = sum(1 for it in g_items if it.get("path") in selected_set)
                all_selected = len(g_items) > 0 and sel_in_group == len(g_items)
                # 组头（底色横条）
                page.append({
                    'component': 'VRow',
                    'props': {
                        'class': 'ma-0 mt-3 mb-1 py-2 px-2 d-flex align-center',
                        'style': 'background: rgba(2,136,209,0.10); border-radius: 8px;'
                    },
                    'content': [
                        {
                            'component': 'VCol',
                            'props': {'cols': 12, 'md': 1},
                            'content': [
                                {
                                    'component': 'VCheckboxBtn',
                                    'props': {'model-value': all_selected, 'density': 'compact'},
                                    'events': {
                                        'click': {
                                            'api': 'plugin/OrphanFileCleaner/toggle_group',
                                            'method': 'get',
                                            'params': {'dir': d, 'apikey': settings.API_TOKEN}
                                        }
                                    }
                                }
                            ]
                        },
                        {
                            'component': 'VCol',
                            'props': {'cols': 12, 'md': 7},
                            'content': [
                                {
                                    'component': 'div',
                                    'props': {'class': 'text-subtitle-2 font-weight-bold text-primary'},
                                    'text': f"📁 {d}"
                                },
                                {
                                    'component': 'div',
                                    'props': {'class': 'text-caption text-grey'},
                                    'text': f"{len(g_items)} 项 · 已勾选 {sel_in_group}"
                                }
                            ]
                        },
                        {
                            'component': 'VCol',
                            'props': {'cols': 12, 'md': 4},
                            'content': [
                                self._btn("删除本组全部", 'plugin/OrphanFileCleaner/delete_dir',
                                          {'dir': d, 'confirm': 'yes'}, 'error', 'tonal')
                            ]
                        },
                    ]
                })
                # 条目行（四列对齐，已勾选浅绿背景）
                for it in sorted(g_items, key=lambda x: x.get("path", "")):
                    path = it.get("path", "")
                    ftype = "🗂" if it.get("is_dir") else "📄"
                    size = self._fmt_size(it.get("size", 0))
                    checked = path in selected_set
                    row_style = 'background: rgba(76,175,80,0.12); border-radius: 6px;' if checked else ''
                    page.append({
                        'component': 'VRow',
                        'props': {'class': 'ma-0 py-1 px-2 d-flex align-center', 'style': row_style},
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 1},
                                'content': [
                                    {
                                        'component': 'VCheckboxBtn',
                                        'props': {'model-value': checked, 'density': 'compact'},
                                        'events': {
                                            'click': {
                                                'api': 'plugin/OrphanFileCleaner/toggle',
                                                'method': 'get',
                                                'params': {'path': path, 'apikey': settings.API_TOKEN}
                                            }
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'div',
                                        'props': {'class': 'text-body-2 text-wrap'},
                                        'text': f"{ftype} {path}"
                                    },
                                    *([{
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'warning',
                                            'variant': 'tonal',
                                            'density': 'compact',
                                            'class': 'mt-1 text-wrap'
                                        },
                                        'text': "⚠ 有硬链接（" + str(len(it.get("hardlinks", []))) + " 个）：\n" + "\n".join(it.get("hardlinks", []))
                                    }] if it.get("has_hardlink") else [])
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 2, 'class': 'text-right'},
                                'content': [
                                    {
                                        'component': 'div',
                                        'props': {'class': 'text-body-2 text-grey'},
                                        'text': size
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    self._btn("删除", 'plugin/OrphanFileCleaner/delete',
                                              {'path': path}, 'error', 'text')
                                ]
                            },
                        ]
                    })

        # ===== 最近处理记录 =====
        if deleted:
            page.append({
                'component': 'VRow',
                'props': {'class': 'mt-4'},
                'content': [
                    {
                        'component': 'VCol',
                        'props': {'cols': 12},
                        'content': [
                            {
                                'component': 'div',
                                'props': {'class': 'text-subtitle-2 font-weight-bold mb-1'},
                                'text': f"最近处理记录（{len(deleted)} 条）"
                            },
                            *[{'component': 'div', 'props': {'class': 'text-caption text-grey'},
                               'text': f"· {d}"} for d in deleted[-10:]]
                        ]
                    }
                ]
            })
        return page

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/scan",
                "endpoint": self.scan_api,
                "methods": ["GET"],
                "summary": "扫描数据目录中的孤儿文件"
            },
            {
                "path": "/delete",
                "endpoint": self.delete_api,
                "methods": ["GET"],
                "summary": "删除指定孤儿文件"
            },
            {
                "path": "/delete_all",
                "endpoint": self.delete_all_api,
                "methods": ["GET"],
                "summary": "删除全部孤儿文件"
            },
            {
                "path": "/delete_selected",
                "endpoint": self.delete_selected_api,
                "methods": ["GET"],
                "summary": "批量删除勾选中的孤儿文件"
            },
            {
                "path": "/toggle",
                "endpoint": self.toggle_api,
                "methods": ["GET"],
                "summary": "切换孤儿文件勾选状态"
            },
            {
                "path": "/toggle_group",
                "endpoint": self.toggle_group_api,
                "methods": ["GET"],
                "summary": "一键选中/取消整个文件夹组"
            },
            {
                "path": "/delete_dir",
                "endpoint": self.delete_dir_api,
                "methods": ["GET"],
                "summary": "一键删除文件夹组内全部孤儿"
            },
            {
                "path": "/clear",
                "endpoint": self.clear_api,
                "methods": ["GET"],
                "summary": "清空扫描结果"
            },
        ]

    # ==================== 核心逻辑 ====================

    def _collect_roots(self) -> set:
        """收集全部下载器种子的内容根路径（已做容器路径映射）"""
        roots = set()
        try:
            services = DownloaderHelper().get_services()
        except Exception as e:
            logger.error(f"孤儿文件清理：获取下载器失败：{e}")
            return roots
        for name, info in services.items():
            inst = info.instance
            mod = type(inst).__module__
            try:
                torrents, err = inst.get_torrents()
            except Exception as e:
                logger.warning(f"孤儿文件清理：{name} 获取种子失败：{e}")
                continue
            if err or not torrents:
                continue
            for t in torrents:
                try:
                    if "transmission" in mod:
                        ddir = str(t.download_dir or "")
                        tname = str(t.name or "")
                        r = os.path.join(ddir, tname) if ddir and tname else ""
                    else:
                        r = str(t.get("content_path") or "")
                        if not r:
                            sp = str(t.get("save_path") or "")
                            tname = str(t.get("name") or "")
                            r = os.path.join(sp, tname) if sp and tname else ""
                    r = self._map_path(r)
                    if r:
                        roots.add(r)
                except Exception:
                    continue
            logger.info(f"孤儿文件清理：{name} 收集种子引用 {len(torrents)} 个")
        return roots

    def _map_path(self, p: str) -> str:
        for pair in (self._paths or "").split(";"):
            if "=" in pair:
                old, new = pair.split("=", 1)
                old, new = old.strip(), new.strip()
                if old and new and p.startswith(old):
                    return new + p[len(old):]
        return p

    def _scan_dir(self, path: str, cr: List[str], base: str, items: List[Dict]):
        """递归扫描目录，cr 为已排序引用集合，孤儿条目加入 items"""
        try:
            with os.scandir(path) as it:
                entries = list(it)
        except Exception:
            return
        for e in entries:
            p = e.path
            i = bisect.bisect_left(cr, p)
            contained = i < len(cr) and (cr[i] == p or cr[i].startswith(p + "/"))
            referenced = i > 0 and (p == cr[i - 1] or p.startswith(cr[i - 1] + "/"))
            if referenced:
                continue
            if contained:
                if e.is_dir(follow_symlinks=False):
                    self._scan_dir(p, cr, base, items)
                continue
            # 孤儿条目
            if e.is_dir(follow_symlinks=False):
                size = self._dir_size(p)
                items.append({"path": p, "dir": base, "is_dir": True, "size": size})
            else:
                try:
                    size = e.stat().st_size
                except Exception:
                    size = 0
                items.append({"path": p, "dir": base, "is_dir": False, "size": size})

    def _dir_size(self, path: str) -> int:
        """递归统计目录大小（文件数上限保护，防止超大目录卡死）"""
        total = 0
        count = 0
        try:
            for dp, dn, fn in os.walk(path):
                for f in fn:
                    try:
                        total += os.path.getsize(os.path.join(dp, f))
                    except Exception:
                        pass
                    count += 1
                    if count > 500000:
                        return total
        except Exception:
            pass
        return total

    def _find_hardlinks(self, items: List[Dict], search_roots: List[str]) -> int:
        """检查孤儿条目的硬链接，在 items 上补充 hardlinks 字段，返回有硬链接的条目数"""
        # 1) 收集每个孤儿条目下的所有文件及其 (dev,ino)（仅 nlink>1）
        orphan_files = {}          # orphan_path -> [(file_path, (dev,ino))]
        key_to_orphans = {}        # (dev,ino) -> set of orphan paths
        for it in items:
            p = it.get("path", "")
            files = []
            if it.get("is_dir"):
                try:
                    for dp, dn, fns in os.walk(p):
                        for fn in fns:
                            files.append(os.path.join(dp, fn))
                except Exception:
                    pass
            else:
                files = [p]
            inode_list = []
            for f in files:
                try:
                    st = os.stat(f)
                    if st.st_nlink > 1:
                        key = (st.st_dev, st.st_ino)
                        inode_list.append((f, key))
                        key_to_orphans.setdefault(key, set()).add(p)
                except Exception:
                    continue
            orphan_files[p] = inode_list

        if not key_to_orphans:
            return 0
        logger.info(f"孤儿文件清理：检查 {len(key_to_orphans)} 个有硬链接的 inode...")

        # 2) 搜索范围内找同 (dev,ino) 的所有路径
        hardlink_map = {key: set() for key in key_to_orphans}
        scanned = 0
        for root in search_roots:
            root = root.strip()
            if not root or not os.path.isdir(root):
                continue
            for dp, dn, fns in os.walk(root):
                for fn in fns:
                    scanned += 1
                    fp = os.path.join(dp, fn)
                    try:
                        st = os.stat(fp)
                        key = (st.st_dev, st.st_ino)
                        if key in hardlink_map:
                            hardlink_map[key].add(fp)
                    except Exception:
                        continue
                    if scanned % 100000 == 0:
                        logger.info(f"孤儿文件清理：硬链接搜索进度 {scanned} 文件...")
                    if scanned > 3000000:
                        logger.warning("孤儿文件清理：硬链接搜索文件数超限(300万)，截断")
                        break
                if scanned > 3000000:
                    break

        # 3) 为每个孤儿条目补充 hardlinks（排除自身路径下的文件）
        hl_count = 0
        for it in items:
            p = it.get("path", "")
            own_files = set(fp for fp, _ in orphan_files.get(p, []))
            hl = set()
            for fp, key in orphan_files.get(p, []):
                for hp in hardlink_map.get(key, set()):
                    if hp not in own_files:
                        hl.add(hp)
            it["hardlinks"] = sorted(hl)
            it["has_hardlink"] = len(hl) > 0
            if hl:
                hl_count += 1
        logger.info(f"孤儿文件清理：硬链接检查完成，{hl_count} 个孤儿条目有硬链接")
        return hl_count

    def scan_api(self) -> Dict[str, Any]:
        """扫描 API：收集引用 + 遍历配置目录，结果存插件数据"""
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        dirs = [d.strip() for d in (self._scan_dirs or "").splitlines() if d.strip()]
        if not dirs:
            return {"success": False, "message": "未配置扫描目录"}
        logger.info("孤儿文件清理：开始扫描，收集下载器种子引用集合...")
        cr = sorted(self._collect_roots())
        logger.info(f"孤儿文件清理：引用集合 {len(cr)} 个，开始遍历目录...")
        items = []
        for d in dirs:
            if not os.path.isdir(d):
                logger.warning(f"孤儿文件清理：目录不存在或不可访问：{d}")
                continue
            before = len(items)
            self._scan_dir(d, cr, d, items)
            logger.info(f"孤儿文件清理：{d} 发现 {len(items) - before} 个孤儿")
        result = {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "items": items,
            "total": len(items),
        }
        self.save_data("result", result)
        self._sel = []
        # 检查硬链接
        hl_count = 0
        if self._hardlink_search:
            search_roots = [d.strip() for d in (self._scan_dirs or "").splitlines() if d.strip()]
            extra = [d.strip() for d in (self._hardlink_search_dirs or "").splitlines() if d.strip()]
            hl_count = self._find_hardlinks(items, search_roots + extra)
            self.save_data("result", {"time": result["time"], "items": items, "total": len(items)})
        logger.info(f"孤儿文件清理：扫描完成，共 {len(items)} 个孤儿文件" + (f"，其中 {hl_count} 个有硬链接" if self._hardlink_search else ""))
        if self._notify:
            self._send_notify(f"{self._notify_title_prefix}: 孤儿文件清理-扫描完成\n"
                              f"发现 {len(items)} 个孤儿文件，请到插件页面查看/删除")
        return {"success": True, "total": len(items), "items": items}

    def delete_api(self, path: str) -> Dict[str, Any]:
        """删除单个孤儿"""
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        if not path:
            return {"success": False, "message": "未指定路径"}
        # 路径安全校验：必须位于已配置扫描目录范围内
        if not self._is_in_scan_dirs(path):
            return {"success": False, "message": "路径不在扫描目录范围内，已拒绝"}
        ok, msg = self._delete_path(path)
        if ok:
            self._append_deleted(f"已{'移入回收站' if self._delete_mode == 'trash' else '删除'}: {path}")
            self._mark_done(path)
            # 同步从选中集合移除
            selected = self._sel
            if path in selected:
                selected.remove(path)
                self._sel = selected
        return {"success": ok, "message": msg}

    def delete_all_api(self, confirm: str = "") -> Dict[str, Any]:
        """删除全部孤儿（需 confirm=yes）"""
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        saved = self.get_data("result") or {}
        items = saved.get("items") or []
        if not items:
            return {"success": False, "message": "当前无扫描结果"}
        ok_count = 0
        for it in items:
            p = it.get("path", "")
            if not self._is_in_scan_dirs(p):
                continue
            if self._delete_path(p)[0]:
                ok_count += 1
        saved["items"] = []
        self.save_data("result", saved)
        self._sel = []
        self._append_deleted(f"批量处理完成：成功 {ok_count}/{len(items)} 个")
        logger.info(f"孤儿文件清理：批量删除完成 {ok_count}/{len(items)}")
        if self._notify:
            mode = "移入回收站" if self._delete_mode == "trash" else "彻底删除"
            self._send_notify(f"{self._notify_title_prefix}: 孤儿文件清理-批量处理\n"
                              f"成功 {ok_count}/{len(items)} 个（{mode}）")
        return {"success": True, "ok": ok_count, "total": len(items)}

    def delete_selected_api(self, confirm: str = "") -> Dict[str, Any]:
        """批量删除勾选中的孤儿（需 confirm=yes）"""
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        selected = self._sel
        if not selected:
            return {"success": False, "message": "未勾选任何孤儿文件"}
        ok_count = 0
        for p in selected:
            if not self._is_in_scan_dirs(p):
                continue
            if self._delete_path(p)[0]:
                ok_count += 1
        self._sel = []
        # 从结果中移除已处理的条目
        saved = self.get_data("result") or {}
        saved["items"] = [it for it in saved.get("items", []) if it.get("path") not in selected]
        saved["total"] = len(saved["items"])
        self.save_data("result", saved)
        self._append_deleted(f"删除勾选项完成：成功 {ok_count}/{len(selected)} 个")
        logger.info(f"孤儿文件清理：批量删除勾选项完成 {ok_count}/{len(selected)}")
        if self._notify:
            mode = "移入回收站" if self._delete_mode == "trash" else "彻底删除"
            self._send_notify(f"{self._notify_title_prefix}: 孤儿文件清理-批量删除勾选\n"
                              f"成功 {ok_count}/{len(selected)} 个（{mode}）")
        return {"success": True, "ok": ok_count, "total": len(selected)}

    def toggle_api(self, path: str) -> Dict[str, Any]:
        """切换勾选状态"""
        if not path:
            return {"success": False, "message": "未指定路径"}
        selected = self._sel
        if path in selected:
            selected.remove(path)
            msg = "已取消勾选"
        else:
            selected.append(path)
            msg = "已勾选"
        self._sel = selected
        return {"success": True, "message": msg, "selected_count": len(selected)}

    def toggle_group_api(self, dir: str) -> Dict[str, Any]:
        """一键选中/取消整个文件夹组的孤儿"""
        if not dir:
            return {"success": False, "message": "未指定文件夹"}
        saved = self.get_data("result") or {}
        items = saved.get("items") or []
        group_paths = [it.get("path") for it in items if it.get("dir") == dir]
        if not group_paths:
            return {"success": False, "message": "该文件夹无孤儿条目"}
        selected = self._sel
        group_set = set(group_paths)
        # 组内全部已选中 → 取消全部；否则全选
        if all(p in selected for p in group_paths):
            selected = [p for p in selected if p not in group_set]
            msg = f"已取消勾选 {len(group_paths)} 项"
        else:
            for p in group_paths:
                if p not in selected:
                    selected.append(p)
            msg = f"已勾选 {len(group_paths)} 项"
        self._sel = selected
        logger.info(f"孤儿文件清理：[{dir}] 组级勾选 {msg}")
        return {"success": True, "message": msg, "selected_count": len(selected)}

    def delete_dir_api(self, dir: str, confirm: str = "") -> Dict[str, Any]:
        """一键删除文件夹组内全部孤儿（需 confirm=yes）"""
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        if not dir:
            return {"success": False, "message": "未指定文件夹"}
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        saved = self.get_data("result") or {}
        items = saved.get("items") or []
        group_items = [it for it in items if it.get("dir") == dir]
        if not group_items:
            return {"success": False, "message": "该文件夹无孤儿条目"}
        ok_count = 0
        for it in group_items:
            p = it.get("path", "")
            if not self._is_in_scan_dirs(p):
                continue
            if self._delete_path(p)[0]:
                ok_count += 1
        # 从结果移除该组 + 同步清理勾选集合
        saved["items"] = [it for it in items if it.get("dir") != dir]
        saved["total"] = len(saved["items"])
        self.save_data("result", saved)
        group_paths = set(it.get("path") for it in group_items)
        selected = self._sel
        self._sel = [p for p in selected if p not in group_paths]
        self._append_deleted(f"[{dir}] 组级删除完成：成功 {ok_count}/{len(group_items)} 个")
        logger.info(f"孤儿文件清理：[{dir}] 组级删除 {ok_count}/{len(group_items)}")
        if self._notify:
            mode = "移入回收站" if self._delete_mode == "trash" else "彻底删除"
            self._send_notify(f"{self._notify_title_prefix}: 孤儿文件清理-删除文件夹组\n"
                              f"[{dir}] 成功 {ok_count}/{len(group_items)} 个（{mode}）")
        return {"success": True, "ok": ok_count, "total": len(group_items)}

    def clear_api(self, confirm: str = "") -> Dict[str, Any]:
        """清空扫描结果"""
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        self.save_data("result", {"time": "", "items": [], "total": 0})
        self._sel = []
        return {"success": True}

    # ==================== 工具 ====================

    def _is_in_scan_dirs(self, path: str) -> bool:
        dirs = [d.strip() for d in (self._scan_dirs or "").splitlines() if d.strip()]
        for d in dirs:
            if path == d or path.startswith(d.rstrip("/") + "/"):
                return True
        return False

    def _get_trash_dir(self, path: str) -> str:
        if self._trash_dir.strip():
            return self._trash_dir.strip()
        dirs = [d.strip() for d in (self._scan_dirs or "").splitlines() if d.strip()]
        for d in dirs:
            if path == d or path.startswith(d.rstrip("/") + "/"):
                return os.path.join(d, ".orphan_trash")
        return "/tmp/orphan_trash"

    def _delete_path(self, path: str) -> Tuple[bool, str]:
        """按配置模式删除路径，返回 (是否成功, 说明)"""
        if not os.path.exists(path):
            return False, "路径不存在"
        try:
            if self._delete_mode == "trash":
                trash = self._get_trash_dir(path)
                os.makedirs(trash, exist_ok=True)
                target = os.path.join(trash, os.path.basename(path.rstrip("/")) or "orphan")
                base, ext = os.path.splitext(target)
                n = 1
                while os.path.exists(target):
                    target = f"{base}_{n}{ext}"
                    n += 1
                os.rename(path, target)
                logger.info(f"孤儿文件清理：已移入回收站：{path} -> {target}")
                return True, f"已移入回收站 {target}"
            else:
                if os.path.isdir(path) and not os.path.islink(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
                logger.info(f"孤儿文件清理：已彻底删除：{path}")
                return True, "已彻底删除"
        except Exception as e:
            logger.error(f"孤儿文件清理：删除失败 {path}：{e}")
            return False, f"删除失败：{e}"

    def _append_deleted(self, msg: str):
        deleted = self.get_data("deleted") or []
        deleted.append(f"{datetime.now().strftime('%m-%d %H:%M')} {msg}")
        self.save_data("deleted", deleted[-200:])

    @staticmethod
    def _fmt_size(size) -> str:
        try:
            s = float(size or 0)
        except Exception:
            s = 0.0
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if s < 1024:
                return f"{s:.1f}{unit}"
            s /= 1024
        return f"{s:.1f}PB"

    def _mark_done(self, path: str):
        saved = self.get_data("result") or {}
        for it in saved.get("items", []):
            if it.get("path") == path:
                it["done"] = True
                break
        self.save_data("result", saved)

    def _send_notify(self, text: str):
        self.post_message(title="孤儿文件清理", text=text)

    @staticmethod
    def _btn(text: str, api: str, payload: dict, color: str, variant: str = "tonal") -> Dict:
        params = dict(payload)
        params['apikey'] = settings.API_TOKEN
        return {
            "component": "VBtn",
            "props": {"color": color, "variant": variant, "size": "small", "block": True},
            "text": text,
            "events": {
                "click": {
                    "api": api,
                    "method": "get",
                    "params": params,
                }
            },
        }