# CPA-CLEAR

一个用于批量检测 [CPA](https://github.com/router-for-me/CLIProxyAPI) 账号状态并整理本地认证文件的 Python 脚本。

脚本会先从远端 CPA 管理接口拉取账号列表，再逐个探测账号的使用状态，识别出：

- `401` / 不可用账号
- 配额已用尽或低于阈值的账号

识别完成后，会把对应的本地 JSON 文件移动到输出目录，并导出结果清单。

## 适用场景

当你本地有一批认证文件，需要配合远端管理接口快速筛出失效账号或 quota 异常账号时，可以用这个脚本做一次批量清洗。

## 项目结构

```text
.
├─ delete.py      # 主脚本
└─ config.json    # 配置文件
```

## 运行要求

- Python 3.10+
- 可访问的 CPA 管理接口
- 有效的接口 `token`
- 本地CPA账号目录，例如 `auth-dir/`

## 快速开始

1. 修改根目录下的 `config.json`
2. 配置CPA `auth-dir` 目录至 `input_dir`
3. 运行脚本

```bash
python delete.py
```

## 配置说明

默认读取项目根目录下的 `config.json`。

示例：

```json
{
  "base_url": "https://your-api.example.com/",
  "token": "your-token",
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

常用字段：

- `base_url`：CPA 管理接口地址
- `token`：接口鉴权 token
- `input_dir`：本地待处理账号文件目录
- `output_dir`：输出目录，结果文件和移动后的账号文件都会放这里
- `target_type`：远端账号类型过滤，默认是 `codex`
- `provider`：服务商过滤，为空表示不过滤
- `timeout`：单次请求超时时间，单位秒
- `retries`：请求失败后的重试次数
- `workers`：并发探测线程数
- `recursive`：是否递归扫描 `input_dir` 下的子目录
- `quota_disable_threshold`：quota 剩余比例阈值，`0.0` 表示只按接口明确返回的限额状态判断
- `move_mode`：移动模式，可选 `all`、`401`、`quota`
- `debug`：调试模式，开启后会输出 `401` 邮箱列表，并跳过 `401` 文件移动

## 处理逻辑

脚本大致流程如下：

1. 从远端接口拉取账号列表
2. 根据 `target_type` 和 `provider` 过滤目标账号
3. 调用接口探测账号状态
4. 识别 `401` 账号与 quota 异常账号
5. 导出结果 JSON
6. 按文件名把本地对应账号文件移动到 `output_dir`

本地文件匹配时会优先按以下方式查找：

- 完整文件名匹配
- `name + ".json"` 匹配
- 文件名 stem 匹配

## 输出结果

运行后通常会在 `output_dir` 下看到这些文件：

- `401_accounts.json`：识别出的 `401` / 不可用账号
- `quota_accounts.json`：识别出的 quota 异常账号
- `move_results.json`：文件移动结果

当 `debug=true` 时，还会额外输出：

- `401_emails.txt`：`401` 账号邮箱列表

## 注意事项

- `debug=true` 时，脚本会跳过 `401` 文件的实际移动，只导出结果和邮箱列表。
- `move_mode=401` 只移动 `401` 账号文件。
- `move_mode=quota` 只移动 quota 异常账号文件。
- `move_mode=all` 会同时处理两类账号。
- 如果本地文件名和远端返回的账号名对不上，移动阶段会提示 `local file not found in input_dir`。

## 致谢

本项目在处理思路和 CPA 管理流程上参考项目 [fantasticjoe/cpa-warden](https://github.com/fantasticjoe/cpa-warden)。

## License
[MIT](./LICENSE)