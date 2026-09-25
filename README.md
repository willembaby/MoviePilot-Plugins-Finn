# MoviePilot 自定义插件包

包含两个针对「订阅剧集追更」场景的自定义插件，解决国内影视平台数据与 TMDB 不一致导致的订阅问题。

> 作者信息默认为 `Finn`；如需以自己的名义发布，请修改源码中的 `plugin_author` 与 `author_url` 两处字段（共 2 个插件文件）。

## 插件列表

### 1. 订阅完结交叉校验（SubscribeCompletionCrossCheck）v1.3

**解决痛点**：TMDB 对国产剧/国漫的总集数标注经常滞后（例如某剧 TMDB 只标 12 集，实际有 25 集），导致 MoviePilot 在剧集尚未完结时就判定"已完结"、停止下载后续集数。

**核心能力**：
- **实时守卫**：订阅被判定完结、归档删除之前，交叉校验以下数据源：
  - 豆瓣：总集数 + 平台连载状态（"更新至X集"，聚合自优酷/腾讯视频/爱奇艺等国内播放平台）
  - Bangumi：总集数（动漫收录好）
  - 任一源显示剧集未完结 → 自动否决完结，扩展订阅总集数，继续追更
- **每日兜底扫描**（默认每天 09:00）：扫描近期已完结的订阅历史，发现误完结的自动重建订阅
- **源不可用容错**：豆瓣/Bangumi 接口异常时挂起完结判定、自动重试；连续 N 天不可用才放行并通知，避免误完结和死挂
- **通知**：飞书 Webhook 或系统消息，内容含校验证据（各源集数）

**配置项**：启用、Bangumi 校验开关、兜底扫描时间/范围/条数、源不可用挂起天数、无总集数缓冲集数、飞书 Webhook、通知前缀。

**依赖**：MoviePilot v2.15.6+；容器内需可访问豆瓣接口（MoviePilot 内置模块）与 api.bgm.tv（通过 curl 子进程）。

---

### 2. 订阅日历监控（SubscribeCalendarMonitor）v1.2

**能力**：每日定时检查"当日播出"的订阅剧集是否已下载入库并发送通知（如飞书日报）。使用 TMDB 播出日历 + 媒体库文件状态比对。

**配置项**：启用、检查时间（Cron）、仅有缺失时通知、飞书 Webhook、通知前缀、立即运行一次。

**依赖**：MoviePilot v2（使用内置 TmdbChain / SubscribeChain）。

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