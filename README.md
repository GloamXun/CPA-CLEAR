# CPA-CLEAR

一个用于批量整理 CPA 账号文件的小工具集。

当前项目包含三个核心脚本：

- `delete.py`：从 CPA 管理接口拉取账号状态，识别 `401` / quota 异常账号，并移动本地对应文件
- `transform.py`：把本地邮箱 `json` 转成 sub2api 可导入格式，并先和 sub2api 现有账号按邮箱去重
- `sub2api_detect.py`：直接拉取 sub2api 中的账号，用账号内的 `access_token` 检测 `401`

## 项目结构

```text
.
├─ delete.py
├─ transform.py
├─ sub2api_detect.py
├─ config.json
└─ LICENSE
```

## 运行要求

- Python 3.10+
- 可访问的 CPA 管理接口
- 可访问的 sub2api 管理接口
- 有效的接口 token

## delete.py

`delete.py` 用于筛出 CPA 中已经失效或配额异常的账号，并把本地对应认证文件移动到输出目录。

### 功能

- 拉取 CPA `auth-files`
- 按 `target_type` / `provider` 过滤账号
- 探测账号状态
- 识别 `401` / 不可用账号
- 识别 quota 已用尽或低于阈值的账号
- 导出结果 JSON
- 按文件名匹配并移动本地账号文件

### 运行

```bash
python delete.py
```

### delete.py 配置示例

默认读取根目录 `config.json` 顶层字段。

```json
{
  "base_url": "https://your-cpa-api.example.com/",
  "token": "your-cpa-token",
  "input_dir": "./auth-dir",
  "output_dir": "./output_dir",
  "target_type": "codex",
  "provider": "",
  "timeout": 15,
  "retries": 3,
  "workers": 30,
  "recursive": false,
  "quota_disable_threshold": 0.0,
  "move_mode": "401",
  "debug": false
}
```

### delete.py 输出

- `401_accounts.json`
- `quota_accounts.json`
- `move_results.json`
- `401_emails.txt`：仅在 `debug=true` 时生成

## transform.py

`transform.py` 用于把本地账号文件转换成 sub2api 导入 JSON，并在导入前自动与 sub2api 现有账号按邮箱去重。

### transform.py 现在的处理流程

1. 扫描本地目录下的邮箱 `json` 文件
2. 自动轮询 sub2api 账号管理接口 `/api/v1/admin/accounts/` 的所有分页
3. 提取 sub2api 中已有账号邮箱
4. 用本地文件邮箱与远端邮箱做去重
5. 仅保留 sub2api 中不存在的账号
6. 生成 sub2api 导入包
7. 额外导出去重报告，方便核对哪些账号被跳过

### transform.py 适配的 sub2api 接口

默认会按类似下面的方式自动翻页请求：

```text
https://your-sub2api-host/api/v1/admin/accounts/?page=1&page_size=100
```

请求头使用：

```text
x-api-key: <your-x-api-key>
```
登录 sub2api 的管理员账号后，打开 `F12` 进入 `Network`，找到 `Request URL` 为 `/api/v1/admin/accounts/` 的请求，复制其 `Request Headers` 中的 `x-api-key` 到 `config.json`。

### transform.py 运行

最简单的方式：

```bash
python transform.py
```

如果你只想离线生成导入包，不访问 sub2api：

```bash
python transform.py --skip-remote-dedupe
```

也可以临时通过命令行覆盖配置：

```bash
python transform.py ^
  --input ./auth-dir ^
  --sub2api-base-url https://your-sub2api-api.example.com/ ^
  --sub2api-x-api-key "your-x-api-key"
```

### transform 配置示例

`transform.py` 默认也读取根目录 `config.json`，并优先读取其中的 `transform` 配置段。

```json
{
  "base_url": "https://your-cpa-api.example.com/",
  "token": "your-cpa-token",
  "input_dir": "./auth-dir",
  "output_dir": "./output_dir",
  "target_type": "codex",
  "provider": "",
  "timeout": 15,
  "retries": 3,
  "workers": 30,
  "recursive": false,
  "quota_disable_threshold": 0.0,
  "move_mode": "401",
  "debug": false,
  "transform": {
    "input_dir": "./auth-dir",
    "output_file": "./sub2api_accounts_import.json",
    "report_file": "./sub2api_dedupe_report.json",
    "include_pattern": "*@*.json",
    "exclude_patterns": [
      "merged*.json",
      "import_payload*.json",
      "sub2api_accounts_import*.json",
      "sub2api_dedupe_report*.json"
    ],
    "recursive": false,
    "platform": "openai",
    "account_type": "oauth",
    "concurrency": 3,
    "priority": 50,
    "name_source": "email",
    "name_prefix": "acc",
    "timeout": 15,
    "base_url": "https://your-sub2api-api.example.com/",
    "x_api_key": "your-sub2api-x-api-key"
  }
}
```

### transform.py 输出

- `sub2api_accounts_import.json`：只包含需要新增导入的账号
- `sub2api_dedupe_report.json`：包含统计信息和跳过原因

报告里会区分：

- 已存在于 sub2api 的邮箱
- 本地重复邮箱
- 无法识别邮箱的记录

## 推荐工作流

如果你是日常维护自己的 sub2api 和 CPA，可以按这个顺序：

1. 确认 CPA 认证文件存放目录，本地登录 sub2api 管理员账号获取 `x-api-key`，并更新 `config.json`
2. 运行 `python delete.py`，清理 `401` 或 quota 异常账号文件
3. 先运行 `python transform.py`
4. 打开 `sub2api_dedupe_report.json`，确认哪些邮箱已存在、哪些会新增
5. 将 `sub2api_accounts_import.json` 导入 sub2api


这样你的流程会比较稳定：

- `transform.py` 负责“导入前去重”
- `delete.py` 负责“失效账号清理”

## sub2api_detect.py

`sub2api_detect.py` 会先从 sub2api 拉取所有分页中的账号信息，再对每个账号调用 `/api/v1/admin/accounts/{id}/test`，并根据 `test` 接口返回结果输出所有 `401` 账号。

### 运行

```bash
python sub2api_detect.py
```

### 输出

- `sub2api_detect_output/probe_records.json`：所有探测结果
- `sub2api_detect_output/401_accounts.json`：检测出的 `401` 账号
- `sub2api_detect_output/401_emails.txt`：检测出的 `401` 邮箱列表

### 配置

`sub2api_detect.py` 不需要单独维护一份 sub2api 连接配置，直接复用 `transform` 里的：

- `transform.base_url`
- `transform.x_api_key`

并发、超时、重试等检测参数则继续复用根级配置，例如：

- `timeout`
- `retries`
- `workers`
- `debug`

## 注意事项

- `transform.py` 默认按邮箱去重，本地文件名是邮箱时效果最好
- 如果本地 JSON 自身没有 `email` 字段，脚本会尝试回退使用文件名 stem 作为邮箱
- 如果没有配置 sub2api URL，`transform.py` 会自动退化为离线模式
- `delete.py` 在 `debug=true` 时会跳过 `401` 文件的实际移动，只输出结果
- `sub2api_detect.py` 现在以 sub2api 自己的 `test` 接口结果为准

## 致谢

本项目在处理思路和 CPA 管理流程上参考了项目 [fantasticjoe/cpa-warden](https://github.com/fantasticjoe/cpa-warden)。

## License

[MIT](./LICENSE)
