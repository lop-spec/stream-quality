# Stream Quality

[手机网页](https://lop-spec.github.io/stream-quality/) · 固定负载流式质量 + 有界下载吞吐。**不调用模型，不读取模型账户，不把 Mbps 换算成 tok/s。**

## 快速使用

Node.js 22+，零 npm 依赖：

```sh
node stream-quality-runner.cjs --endpoint https://stream-quality.1781297309.workers.dev
# 通过指定本机 HTTP/mixed 代理；不修改系统代理
node stream-quality-runner.cjs --endpoint https://stream-quality.1781297309.workers.dev --port 1080 --rounds 3
# 自建本地测试源与页面（默认仅 127.0.0.1:8788）
npm start
npm test
```

公网静态页面由 GitHub Pages 托管，测试源由 Cloudflare Worker 承载。网页测的是**当前浏览器网络到该端点**，并不能遍历代理订阅；手机也不等于在测电脑网络。Smart Proxy 使用同源执行器和独立 sing-box，测全部缓存节点，配置、端口、进程与日常核心分离。只有导出 JSON 时结果才写入下载文件；网页不接收订阅、模型凭据或节点密码。

## v1 固定协议

| 项目 | 固定值 / 含义 |
|---|---|
| Profile | `sq-v1-256b-50ms-20s-8mib` |
| SSE | 401 个 256 字节 sample，0…20000ms，每 50ms 1 个；约 5 KiB/s |
| 样本 | 严格连续序号、服务端单调时钟发送时刻、计划时刻；最终 end 必须完整 |
| 首段等待 | 请求发起至第一个完整 sample 到达，不是模型 TTFT |
| 延迟波动 | 相对首段的额外交付延迟 P95−P5；无需校准跨机时钟，不能测绝对单向延迟 |
| 额外停顿 | 接收间隔减去实际发送间隔，取正数；报告最差值、P95、次数 |
| 攒包 | 源发送间隔≥25ms，但客户端到达间隔<5ms 的比例 |
| 流式达标 | 波动≤100ms、最大额外停顿≤500ms、攒包比例≤10%；属本项目工况，不是行业标准 |
| 源有效性 | 实际发送与计划差最大≤250ms，否则标记端点不合格，不归罪节点 |
| 下载 | 固定 8 MiB 伪随机不可压缩数据，不限发送速率，禁止压缩/缓存；每请求≤30s |
| Mbps | 下载首字节至末字节；另存请求到结束的有效 Mbps；不足1秒标为短样本，不声称峰值容量 |
| 复测 | 1 次初筛或全部节点3次轮流复测，始终串行；3/3成功才 verified |
| 流量 | 每节点每轮最多8 MiB下载＋约103 KiB SSE；不含握手/协议开销；无上传测试 |

同一负载能及时承载则均达标，不按微小差异硬排；流式质量与吞吐独立展示。不同版本、端点、边缘位置的成绩不混排。Cloudflare 是 Anycast 就近边缘，节点出口可能落在不同机房；并非固定物理服务器，也**不代表 OpenAI 的路由、排队、限流或可用性**。

## 隔离执行接口

`node stream-quality-runner.cjs --job /absolute/private/job.json`；从 stdin 接收 `{"action":"cancel"}` 后立即取消请求、停止队列并仅清理自己启动的核心，向 stdout 输出 JSONL start/progress/log/result。

作业：`{ endpoint, rounds: 1|3, corePath, config, nodes: [{key, tag}] }`。`config` 是已转换的 sing-box 配置，节点必须已按指纹去重；执行器不自行解析各种订阅格式。运行时重新建立每节点专属 loopback 入站/路由，端口由系统分配，不启动 TUN、系统代理或控制器。缺失节点转换直接报错，不静默过滤。临时凭据配置不进入日志，在 finally 清理；调用方也应清理私有作业文件。没有 config 时可以传 `nodes: [{key, port}]` 使用已有测试通道。

取消或失败不产生新的合格成绩。历史结果保留/展示由宿主负责，Smart Proxy 使用独立新数据仓，不覆盖旧模型历史。源限流、协议不完整和端点漂移单独标注，不混成节点断网。

## 部署 / 费用边界

- Pages 与 Release 只由 GitHub Actions 在测试通过后发布；本地仅测试。Release 含 npm tarball 与 SHA256。
- Worker：在自己的 Cloudflare 账户运行 `npx wrangler deploy`。默认免费档无需数据库、KV、模型或付费资源；本仓库不包含凭据。当前部署使用 Cloudflare 官方 API、已登录浏览器完成。
- Node 源：`HOST=127.0.0.1 PORT=8788 node server.mjs`，公网需自行配置 HTTPS 反代。固定机房可提高不同节点的对照性。
- 公共源每隔离实例最多8个活动响应、每IP每分钟40次测试请求、固定响应体。此内存限制不是全球硬配额或 DDoS 防护；免费 Worker 也有请求/CPU额度，不能承诺永久无限流量或 SLA。源过载会明确返回429/错误，不能作为节点成绩。
- 免费档 CPU 是否适合该地区/负载需看部署实测；不自动升级付费计划。勿向不可信测试端点发送任何凭据。

## 验收

`npm test` 包含严格帧大小/序号、断流、源抖动识别、网络停顿和攒包、串行调度、3轮交错、取消真实 socket、完整20秒本地代理链路与8MiB下载。真实端点需额外执行 CLI/网页实测，构建通过不等于网络可用。

MIT License.
