# 采集台 · Ingest Desk

一个完全本地的行情采集演示：模拟源端支持长连接与按序号补取；接收器、缺口补取器、入库器是三个独立进程，各自一个 SQLite 文件；页面可以沿任意一条数据看完「接收 → 补取 → 裁决 → 落盘」的全程，也可以随时暂停、恢复、触发故障，最后与源端完整清单逐号对账。

不连接任何真实行情。源端是 `server/source.mjs` 里确定性生成的 fixture。

```bash
node server/index.mjs          # http://127.0.0.1:8787
npm test                       # 42 项：纯逻辑 + 三个进程契约 + 端到端
```

需要 Node ≥ 22.5（使用内置 `node:sqlite` 与 `node:test`）。应用本身零运行时依赖。

---

## 1. 四个进程，三个库

```
 source (fixture, 8899)  ──/feed WS──►  receiver ──HTTP /frames──  backfiller
   epoch / release                       receiver.db   ▲                backfill.db
   manifest                              identity      └──replay WS─────┘
                                         order
                                         coverage      ◄──GET /decisions── lander
                                         conflict                          lander.db
```

| 进程 | 唯一职责 | 存储 | 崩溃/暂停后的结果 |
|---|---|---|---|
| `source` | 发布时钟 + 每 socket 独立 epoch/release + 清单 | 内存 fixture | 只是模拟供应商 |
| `receiver` | 连接身份、帧隔离、业务顺序、覆盖等级、冲突封存 | `data/receiver.db` | 丢失「尚未确认的帧」，不会改写已落定的号 |
| `backfiller` | 看缺口表开按序号补取会话，把回放帧重新喂给接收器 | `data/backfill.db` | 重启后仍知道哪些区间补过、哪些试过几次 |
| `lander` | 按批次拉取接收器判定并事务落盘，崩溃后恢复 | `data/lander.db` | 未提交批次整批回滚；已提交未确认的逐行核对再确认 |

页面上的 `暂停` 会同时冻结三者（`POST /api/pause`），`恢复` 后接收器从**持久游标**重开一条长连接，而不是从内存位置继续。

---

## 2. 为什么「连接身份、业务顺序、落盘进度」必须分成三份

| 存什么 | 存在哪 | 它回答的问题 | 它**不**知道什么 |
|---|---|---|---|
| 连接身份 | `receiver.db · connections` | 这条 socket 是谁、哪个 epoch、有没有声明 basis | 业务进行到几号 |
| 业务顺序 | `receiver.db · coverage` / `gaps` | 每个 global 有没有内容、是 firm 还是 tentative | 内容来自哪条连接的身份语义 |
| 落盘进度 | `lander.db · batches` / `landed` / `amendments` | 哪些字节已经提交并确认（acked） | 接收端还认不认这个内容 |

`frames` 表是唯一把两者连起来的地方：一帧同时带 `conn_id + epoch + release + global`。于是：

- **旧连接还在发数据**，最多改写它自己 `conn_id` 的编号空间；它的 status 通知也只作用于它自己（`onStatus` / `retire` 都按 `line.connId`，`onClose` 只有「当前线」能触发重连）。新连接的状态不会被旧线的迟到通知关掉。
- **源端重置序号**（新 epoch）只改变 `release → global` 的解释，不改写已落定的 `coverage`。
- **入库器崩溃**只影响落盘进度，接收覆盖与补取会话不受影响。

### 身份模型：basis 而不是「猜位置」

每个 socket 在 `hello` 里声明 `basis = { epoch, releaseStart, globalStart, source }`；`release` 只有通过这条声明才能变成 global：

```js
global = basis.globalStart + (release - basis.releaseStart)
```

没有 basis 的帧 → 隔离（`no-basis`）；epoch 与 basis 不符 → 隔离（`epoch-mismatch`）；release 低于 `releaseStart` → 隔离（`release-below-basis`）；内容指纹与声称不符 → 拒收（`fp-mismatch`）。一条连接被 `retired` 后，它后续的帧一律 `retired-connection` 拒收 —— 这是 `hold` 策略能做「先停旧线」的依据。

补取会话同样要先在接收器 `register` 自己的 basis，之后的 `rebase`（重编号）也先注册再发帧，所以补取期间的序号重置不会把帧塞进错误的号。

---

## 3. 两种重连策略：延迟 vs 补齐成本

页面右上可直接切换，`POST /api/policy`。两者都保证最终正确，差别在**接收延迟**和**补取成本**由谁承担。

### `overlap`（默认）

旧线降级时立刻开新线；旧线之后迟到补发的尾部帧按 **tentative（暂定）** 收下：

- 接收游标能马上越过这些号（延迟最低）；
- 但 tentative 内容不能单独算数 —— 补取器会把它们当作 `verify` 区间，用按序号补取确证，一致则提升为 `firm`；
- 若尾部内容与新线冲突，两条变体都被封存，进入裁决。

### `hold`

先用状态通知退休旧线，再让补取去填：

- 旧线的尾部帧全部 `retired-connection` 拒收，不会有任何猜测；
- 洞一直空着直到补取完成（延迟更高，`receiver.gaps` 可见）；
- 换来的好处是没有「先收下再确证」的二次工作，也没有尾部引发的冲突。

注意洞不是等「后面某一号到了才发现」：新线在 `hello` 里声明 `globalStart` 时，接收器立刻把 `cursor+1 … globalStart-1` 标成显式缺口（`gaps.marked`，`why: live-frontier`）。这条 socket 在身份上就不可能补送这段历史，缺口表因此马上有活可派，补取器不必等到流末尾。

页面的「旧连接处置策略」面板实时显示两种策略的实际成本：`origins`（live / tail / replay 各贡献多少帧）、`rejected`（按身份规则拒收多少）、`gapsFilled`（补了多少洞）。同一份 chaos 数据在两种策略下，这些数字不同，而最终对账结果相同。

---

## 4. 页面可触发的三类故障

全部通过控制 API 打到源端或入库器上，接收器与补取器保持运行：

| 故障 | 触发 | 演示什么 |
|---|---|---|
| 旧连接沉默 / 迟到尾部 | 故障台「让旧连接沉默」或 chaos 预设自动 | 两路短暂重叠；尾部为 tentative；状态通知只作用于旧线 |
| 补取期间源端重置序号 | 故障台「安排重编号」 | 新 epoch + 新 basis；`rebase` 先注册再发帧；无 basis 的静默重编号会被隔离 |
| 两条连接内容冲突 | 故障台「制造冲突」（让旧线迟到的尾部在新线已占的号上给出不同内容；`hold` 策略下没有尾部，故不适用） | 两个变体封存 → 按 `replay > live > tail` 裁决 → 落盘行修订 |
| 入库器崩溃 | 故障台两个按钮；chaos 预设还会在约 4.2s 自动安排一次 `mid` | `staging` 整批回滚 / `committed` 未 ack 逐行核对后确认 |

裁决规则：先按来源等级（replay 3 > live 2 > tail 1），同级再按 epoch 大者、再按帧 id 大者。只有「存在严格更高的等级，且该等级内容唯一」的组会在 1.4s 窗口后自动结算；真正有歧义的组保持 `pending`，直到页面点「结算」。**到达顺序永远不参与裁决。**

---

## 5. 「何时足以确认完整」——四个互不代偿的判据

页面左侧的清单逐项打勾，四项全过才给出最终结论。它们是四个不同进程的结论，任何一项都不能被另一项替代：

1. **receivedComplete**（接收器）
   每个 global 都有内容，且游标从 1 连续到 total。`tentative` 也算「有内容」。它只说明「没有洞」，不说明内容可信。

2. **verifiedComplete**（接收器）
   每个 global 都是 `firm`：没有未确证的尾部猜测，没有 `pending` 冲突。它说明「业务顺序上的每个号都有可信内容」，但还没说明字节已经落盘。

3. **落盘已确认**（入库器）
   每个 global 都有 `acked=1` 的行，`ackedCursor == total`，且不存在 `provisional` 行。注意这是入库器自己的连续游标，不是接收器的游标 —— 入库器可以落后，也可以因为缺口停在某个号上。

4. **与源端清单一致**（对账）
   - 逐号：每个 global 的 `fp` 与源端清单相同，没有 `missing / content / unacked / provisional / extra`；
   - 全流：`manifestFp(盘上按 global 排序的行)` 等于源端 `totalFp`。两边用的是**同一个函数**（`server/shared.mjs · manifestFp`），所以相等意味着同一份字节，而不只是同样多行。

> 只有当 1–4 同时成立，采集台才说「这一轮完整」。
> 任意一项单独成立都不够：接收完整可能全是猜测；已验证完整可能还没落盘；落盘完整可能是 `hold` 策略下补取尚未介入前的一个短暂假象（游标停住时会暴露）；指纹一致但缺号会被 `mismatchCount` 抓住。

最终对照在页面「与源端完整清单对照」面板：逐号差异表 + 两侧全流指纹 + `complete` 判定。

---

## 6. 沿一条数据走完全程

页面「沿一条数据走完全程」面板（或点逐号矩阵任意一格）会展示同一个 global 的四段：

1. **接收**：所有 live/tail 帧，各自的 `conn_id · epoch · release · origin · fp`，以及它是首收、复述一致还是争议。
2. **补取**：replay 帧 + 它所属的补取会话（区间、轮次、用途 `gap`/`verify`、源端声明 vs 接收端结论、备注）。
3. **裁决**：冲突组的全部变体、来源等级、胜者/落选；没有冲突时说明为什么不需要。
4. **落盘**：批次与批次状态、`acked`、`provisional`、`revision`、修订前后的指纹、与源端该号内容的比对。
5. 底部：源端与接收器针对这一号留下的所有事件（时间顺序）。

数据来自 `GET /api/trace/:global`，它是接收器 `trace`、入库器 `landGlobal`、源端清单与源端事件日志的拼合 —— 四个独立来源，不在服务端做任何「统一状态」的缓存。

---

## 7. 目录

```
server/
  shared.mjs      内容指纹、全流指纹、basis、deriveGlobal、feed fixture、预设与故障时间
  db.mjs          SQLite 打开/WAL/事件表/HTTP JSON 小工具
  ws.mjs          极简 WebSocket 服务端
  ws-client.mjs   极简 WebSocket 客户端
  source.mjs      模拟源：发布时钟、每 socket epoch/basis、stall/tail/replay/rebase、清单
  receiver.mjs    身份隔离、覆盖等级(tentative/firm)、缺口、冲突封存与裁决
  backfiller.mjs  缺口→区间会话、verify 区间、失败/重编号/放弃规则
  lander.mjs      staging→committed→acked、崩溃恢复、修订(amendments)、reconcile
  index.mjs       监督四个进程 + 控制 API + 静态页
public/
  index.html      采集台页面
  app.js          渲染与交互（无图表库）
  style.css       深色运维样式
test/
  shared.test.mjs   纯函数契约
  receiver.test.mjs 隔离 / 等级 / 冲突 / 两种完整
  lander.test.mjs   三态批次 / 两种崩溃窗口 / 修订 / 对账
  e2e.test.mjs      真起四个进程，steady 与 chaos 各跑一轮并对账
```

运行时文件（`data/*.db`、`.render/`、日志）已在 `.gitignore` 中。

---

## 8. 边界

- 源端是 fixture，不是交易所协议；`ws.mjs` / `ws-client.mjs` 只实现了本演示需要的帧类型。
- 进程间是明文本地 HTTP，没有鉴权；只监听 `127.0.0.1`。
- 补取是「按 global 区间」而不是行情常见的「按时间/序列号区间」；换到真实源时，`basis` 与 `deriveGlobal` 是唯一需要替换的映射层。
- 演示节奏由 `FEED_INTERVAL_MS` 控制；测试用 6ms 跑完两轮，默认 55ms 便于肉眼观察。
