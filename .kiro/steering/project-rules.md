# 项目统一规则（AI 必须严格遵守）

## 项目概述

BSC Token Scanner v4 — 扫描 four.meme 平台上新发行的 BSC 代币，四级管线智能筛选后推送到 Telegram，可选自动交易。

- 主要语言：Python
- 入口文件：scanner.py（扫描器）、trader.py（交易模块）
- 配置文件：config.json（运行时配置，不入库）、config.example.json（配置模板）
- 数据库：tokens.db（SQLite，已推送代币记录）
- 日志：scanner.log

## 所有修改必须遵循

1. 文案、文档、日志、代码必须完全同步修改
2. 任何一处改动，相关所有地方必须一起改，不能遗漏
3. 保持项目整体一致性，不允许出现版本、命名、格式不统一
4. 输出必须完整、可直接使用，不省略关键代码
5. 若有不确定的地方，必须主动询问，不自行假设

## 代码规范

- Python 风格遵循 PEP 8
- 变量名、函数名统一 snake_case
- 注释使用中文，清晰描述意图
- 结构保持一致，新增函数放在同类函数附近
- 筛选阈值定义在 scanner.py 顶部常量区，不要硬编码在函数内部
- 配置参数通过 config.json 读取，支持热更新

## 文案规范

- 用户可见文案（Telegram 推送、日志）使用中文
- 语气统一、术语统一、格式统一
- README.md 与实际代码行为保持同步

## 关键架构约束

- 四级筛选管线逐级收窄：初筛 → 详情筛 → K线筛 → 链上行为筛
- API 调用需注意限流：DexScreener ~300 req/min、GeckoTerminal ~30 req/min、BSCScan ~5 req/s
- GeckoTerminal 作为 DexScreener 的备选数据源，需实现自动退避重试
- 敏感信息（私钥、API Key、Bot Token）不得硬编码，通过环境变量或 config.json 传入
- config.json 已在 .gitignore 中，不要提交到版本库
