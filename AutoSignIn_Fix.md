# AutoSignIn 签到判定修复说明

> 针对 MoviePilot AutoSignIn 插件（v2.9.15，作者 thsrite）「签到未确认」误报问题的本地补丁说明。
> 修改对象为 **AutoSignIn 插件**（第三方插件，非本仓库自研插件），此处仅留存修复方法与可重新部署的补丁内容，供容器重建后重新应用。

---

## 一、问题背景

AutoSignIn 对站点执行签到后，会用 `result.py` 的 `has_signin_evidence()` 判定页面是否「签到成功」。实测发现：

- **海棠（htpt.cc）**、**PT时间（pttime.org）** 等站点**其实已经签到成功**（页面明确显示「已签到」），但插件却标记为「登录成功，签到未确认」。
- 根因：原判定逻辑中「带签到/补签按钮 → 视为尚未操作」这一检查**先于**「已签到文案」检查。当签到成功页面里残留了补签/签到按钮时，页面会被误判为「未确认」。

## 二、修复内容

文件：`/app/app/plugins/autosignin/result.py`（容器内）

调整 `has_signin_evidence()` 的判定顺序，从「按钮检测优先」改为「完成文案优先」：

1. **先排除**明确的失败/否定提示（`签到失败`、`未签到成功`、`尚未签到` 等）。
2. **再优先确认**页面已完成的签到文案（`已签到`、`签到成功`、`已打卡`、`无需再签`、`already attend` 等），命中即视为已签到。
3. **最后**才检查带提交按钮的签到表单（视为「尚未操作」）。

这样既能正确识别已签到成功的页面，又不会因页面上残留的补签/签到按钮把完成状态误判为未完成；同时「未签到+签到按钮」仍正确判为未完成。

## 三、验证结果（真实 cookie 实测）

| 站点 | 修复前 | 修复后 | 说明 |
|---|---|---|---|
| 海棠 htpt.cn | 签到未确认（误报） | ✅ 签到成功 | 页面有「已签到」td + 残留签到按钮 |
| PT时间 pttime.org | 签到未确认（误报） | ✅ 签到成功 | 返回「拒绝访问：已签到，无需再签」 |
| 观众 audiences.me | 签到未确认 | 未签到（正确） | Cloudflare 挑战拦截，确实未签到 |
| 猪猪 piggo.me | 签到失败 | 签到失败（正确） | 被 SafeLine WAF 拦截 |

单测 9 个场景全过，关键覆盖：`已签到+残留按钮 → True`、`未签到+按钮 → False`。

## 四、补丁文件

修复后的完整 `result.py` 见本仓库伴生文件 **`patches/autosignin/result.py`**（与容器内已部署版本一致），覆盖到容器内 `/app/app/plugins/autosignin/result.py` 即可。

## 五、加载与验证（容器内）

```bash
# 1. 覆盖文件
docker cp result.py moviepilot-v2:/app/app/plugins/autosignin/result.py
# 2. 校验语法
docker exec moviepilot-v2 python -m py_compile /app/app/plugins/autosignin/result.py
# 3. 清理旧 pyc 缓存
docker exec moviepilot-v2 rm -f /app/app/plugins/autosignin/__pycache__/result.cpython-312.pyc
# 4. 重新加载插件（query param token 有效，Bearer 无效）
curl -s -X GET "http://127.0.0.1:3000/api/v1/plugin/reload/AutoSignIn?token=<API_TOKEN>"
# 5. 用真实 cookie 重跑判定验证翻转（海棠应变为 True）
```

## 六、持久化注意

- 该修改写入容器内 `/app/app/plugins/autosignin/result.py`，**容器重建后会被覆盖丢失**。
- 持久化方法：将修复后的文件同步到容器持久化挂载，或保留本仓库伴生补丁作为「容器重建后重新部署」的依据。
- 该修改属于对第三方插件的本地定制，**不在本仓库自研插件范围内**，请按需维护。