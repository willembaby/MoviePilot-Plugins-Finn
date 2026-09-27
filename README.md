# MoviePilot 自定义插件包

包含三个针对「订阅剧集追更」与「下载器维护」场景的自定义插件，解决国内影视平台数据与 TMDB 不一致导致的订阅问题，以及 Transmission 种子数据文件丢失后的空壳任务清理。

> 作者信息默认为 `Finn`；如需以自己的名义发布，请修改源码中的 `plugin_author` 与 `author_url` 两处字段（共 2 个插件文件）。

## 插件列表

### 1. 订阅完结交叉校验（SubscribeCompletionCrossCheck）v1.6

**解决痛点**：TMDB 对国产剧/国漫的总集数标注经常滞后（例如某剧 TMDB 只标 12 集，实际有 25 集），导致 MoviePilot 在剧集尚未完结时就判定"已完结"、停止下载后续集数。

**核心能力**：
- **实时守卫**：订阅被判定完结、归档删除之前，交叉校验以下数据源：
  - 豆瓣：总集数 + 平台连载状态（"更新至X集"，聚合自优酷/腾讯视频/爱奇艺等国内播放平台）
  - Bangumi：总集数（动漫收录好）
  - 任一源显示剧集未完结 → 自动否决完结，扩展订阅总集数，继续追更
- **每日兜底扫描**（默认每天 09:00）：扫描近期已完结的订阅历史，发现误完结的自动重建订阅
- **源不可用容错**：豆瓣/Bangumi 接口异常时挂起完结判定、自动重试；连续 N 天不可用才放行并通知，避免误完结和死挂
- **国内剧以豆瓣为准**（v1.4）：豆瓣显示已完结而 TMDB 集数虚高时，自动收缩订阅总集数促使其正常完结归档；国外剧以 TMDB 为准不干预
- **分季条目防误判**（v1.5）：豆瓣集数与订阅集数差距过大时判定为分季/拆分条目，自动跳过，保护连载年番不被误完结
- **搜索兜底**（v1.6）：豆瓣 tv_search 搜不到部分新版条目（如《将夜》2026版）时，改用聚合搜索兜底，仅取电视剧条目，解决漏处理
- **通知**：飞书 Webhook 或系统消息，内容含校验证据（各源集数）

**配置项**：启用、Bangumi 校验开关、兜底扫描时间/范围/条数、源不可用挂起天数、无总集数缓冲集数、飞书 Webhook、通知前缀。

**依赖**：MoviePilot v2.15.6+；容器内需可访问豆瓣接口（MoviePilot 内置模块）与 api.bgm.tv（通过 curl 子进程）。

---

### 2. 订阅日历监控（SubscribeCalendarMonitor）v1.2

**能力**：每日定时检查"当日播出"的订阅剧集是否已下载入库并发送通知（如飞书日报）。使用 TMDB 播出日历 + 媒体库文件状态比对。

**配置项**：启用、检查时间（Cron）、仅有缺失时通知、飞书 Webhook、通知前缀、立即运行一次。

**依赖**：MoviePilot v2（使用内置 TmdbChain / SubscribeChain）。

### 3. 种子文件清理（TorrentFileCleaner）v1.0

**解决痛点**：手工清理或迁移媒体文件后，Transmission 中残留“数据文件已被删除”的空壳种子任务，占用下载器资源、影响做种统计。

**核心能力**：
- 定时扫描 Transmission 全部种子，逐一检查数据文件/目录是否仍存在于下载目录
- 数据文件不存在的“空壳种子”自动从 Transmission 移除任务（**仅删任务，不删文件——文件已不存在**）
- 兼容 `.part` 后缀（TR 开启 rename-partial-files 时未完成下载不误判）
- 仅处理 Transmission 类型下载器，不影响 qBittorrent
- 飞书 Webhook 通知清理结果

**配置项**：启用、执行周期（Cron，默认每 6 小时）、发送通知、飞书 Webhook、通知前缀、立即运行一次。

**依赖**：MoviePilot v2（使用内置 DownloaderHelper）；MoviePilot 容器与 Transmission 需共享下载目录挂载（本插件通过 `download_dir + name` 路径检查数据文件是否存在）。

---

## 安装方式

### 方式一：zip 本地安装（最简单）

1. 解压本包，或在 MoviePilot 插件页面使用「本地安装/上传 zip」
2. 若不支持 zip 安装：将 `subscribecompletioncrosscheck/` 和 `subscribecalendarmonitor/` 两个目录
   直接复制到 MoviePilot 容器的 `/app/app/plugins/` 下
3. 在 MoviePilot 的 `systemconfig` 表（PostgreSQL）的 `UserInstalledPlugins` 配置中
   追加插件类名：`SubscribeCompletionCrossCheck`、`SubscribeCalendarMonitor`
4. 重启 MoviePilot 容器，插件市场即可看到两个插件并配置

### 方式二：GitHub 插件仓库（推荐，支持自动更新）

1. 将本包内容推送到 GitHub 仓库（根目录含两个插件目录即可）
2. MoviePilot → 设置 → 插件 → 自定义插件源，添加仓库地址（`https://github.com/你的用户名/仓库名`）
3. 插件市场即可搜到并一键安装，后续插件更新可从市场拉取

## 注意事项

- 源码不含任何个人配置（Webhook、Token 等均在安装后由配置页面填写）
- 豆瓣数据接口为 MoviePilot 内置模块，无需额外 Key；Bangumi 无需 Key
- 兼容性：基于 MoviePilot v2 API 开发；MoviePilot v3（新架构）可能需要适配

> AI生成