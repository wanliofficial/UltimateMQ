# 背压机制完善 — 方案设计

本文档分析 flowmq 当前背压(backpressure)机制的现状与三处缺口,给出可选方案的
影响面、是否需要 opt-in 开关、以及对现有测试的影响。供审阅后再实现。

## 一、现状:当前背压模型逐层确认

| 层 | 位置 | 机制 | 触发时行为 |
|---|---|---|---|
| 入站发布限速 | `Connection::allowPublish()` / `FLOWMQ_MAX_PUBLISH_RATE` | 令牌桶 | 发布超配额 → MQTT 5 发 DISCONNECT `QUOTA_EXCEEDED`,3.1.1 直接断连 |
| 会话队列上限 | `Session::pushDelivery()` `MAX_QUEUED_MESSAGES=1000` | 硬上限 | **`logDebug` 后静默丢弃新消息** |
| MQTT 5 Receive Maximum | `Session::pushDelivery()` / `releaseBlocked()` | 滑动窗口 | 超窗消息挂 `blockedDeliveries`,ACK 释放后提升 |
| TCP 写超时 | `SecureTransport` `SO_SNDTIMEO` / `FLOWMQ_WRITE_TIMEOUT_MS` | 阻塞写超时 | 慢读者 `writeTimeoutMs` 内无进展 → 断连 |
| 出站最大包 | `Connection::sendPacket()` `clientMaxPacketSize` | 单包上限 | 超包丢弃并释放 inflight slot |

**关键缺口**:会话队列满时静默丢弃,且默认日志级别下**完全不可见**——
`$SYS/broker/messages/sent`、`messages/received` 有计数,但**没有任何"因背压
丢弃"的计数或 `$SYS` 主题**。运维无法知道背压正在发生,也无法在丢弃发生前介入。

## 二、三处缺口的方案

### 缺口 A — 背压可观测性(纯增量,零行为变更)

**问题**:`Session::pushDelivery()` 队列满时仅 `logDebug`,默认日志级别不可见;
无丢弃计数、无 `$SYS` 主题。

**方案**:
1. `Broker` 新增 `std::atomic<uint64_t> totalMessagesDropped`,配套
   `incrementMessagesDropped()` / `sysGetTotalDropped()`(对齐现有
   `totalMessagesSent`/`Received` 的写法)。
2. `Session::pushDelivery()` 丢弃处通过回调/指针调用 broker 计数。
   - 当前 `Session` 已持有 `broker*`(见 `Session.cpp` 多处 `broker->...`),
     直接 `if (broker) broker->incrementMessagesDropped();`。
3. 新增 `$SYS/broker/messages/dropped` 主题:
   - `SysTopics::kTopics` 加一行;`payload()` 加分支返回
     `numberPayload(sysGetTotalDropped())`。
   - `sysTopicValues()` 的 `payload()` 签名需扩展以传入 dropped 计数
     (或新增无参重载)。保持与 sent/received 一致的"仅变化时更新、不写 store、
     不实时 fan-out"语义。
4. **日志级别**:把队列满的 `logDebug` 提升为周期性 `logInfo`(避免刷屏:
   例如每秒最多一条,或复用现有 sweep 节奏)。这是唯一可见性变更,需评估。

**opt-in**:无需开关。`$SYS` 主题仅当 `FLOWMQ_SYS_TOPICS` 启用时发布(现有
`setSysTopics` 门控),计数始终累计但仅在 `$SYS` 开启时暴露——与 sent/received
完全一致。

**影响面**:`SysTopics.cpp/h`、`Broker.cpp/h`、`Session.cpp`。**不改变任何投递
语义**,现有 SessionLifecycle/Broker 测试不受影响。

**测试**:
- 单元测试:构造 session,`pushDelivery` 超过 `MAX_QUEUED_MESSAGES`,断言
  `sysGetTotalDropped()` 递增、订阅 `$SYS/broker/messages/dropped` 的客户端
  收到递增计数。
- 验证:`$SYS` 主题列表含新 topic;`refreshSysTopics` 更新它。

---

### 缺口 B — 队列满的丢弃策略(改变投递语义,需 opt-in)

**问题**:队列满时静默丢弃**新**消息,保留最旧的 1000 条。对实时场景,保留旧数据
可能非预期;对另一些场景(如最新行情)保留最新更合理。

**方案(选项,需你定夺)**:
- **B1(默认,现状)**:满时丢弃新消息。零行为变更,作为默认保持向后兼容。
- **B2(新,opt-in)**:满时丢弃**最旧**消息(保留最新)。通过
  `FLOWMQ_QUEUE_DROP=oldest` 启用,默认 `newest`。

**关键风险与约束**:
- 不能动已入 `inflightMessages` 的 QoS 1/2 消息——它们已发往客户端、持有
  packet id,丢弃会破坏流控窗口(泄漏 slot)与重传。B2 只能从 `pendingDeliveries`
  (尚未发出)与 `blockedDeliveries` 里丢最旧,且丢 QoS>0 的 pending 消息必须
  同步 `releaseInflight` 或直接跳过(它们尚未占 inflight slot,故只需从队列
  移除)。
- 共享订阅:一个 session 多 filter 合并后的最旧消息,语义需明确。
- 与 Receive Maximum 的交互:`blockedDeliveries` 也计入上限(现有设计),
  丢弃策略需同时覆盖两个容器。

**opt-in**:`FLOWMQ_QUEUE_DROP=newest|oldest`,默认 `newest`(现状)。符合
AGENTS.md"opt-in 行为不改变默认路径"。

**影响面**:`Session.cpp/h`、`Server.cpp`(读 env)、可能 `Broker.cpp`。
**需新增测试**:B2 下满队列保留最新、最旧的 QoS0 被丢;B1 下现状回归。

**我的倾向**:B 的收益(选丢弃方向)小于其风险(破坏 QoS 流控)。建议**仅在
A 落地后再评估是否需要 B**,或仅实现一个克制的变体:满时丢最旧**且未入
inflight** 的消息。待你决定。

**已落地(2026-11,完整 B2)**:按"完整 B2"实现,默认不变。实现要点:

- `QueueDropPolicy`(`DropNewest` 默认 / `DropOldest`)定义在 `Broker.h` 命名空间
  作用域(非 `Session` 嵌套),便于只前向声明 `Session` 的翻译单元引用。Broker
  持有该策略,并在 `insertSession`/`restoreState` 两个会话创建路径下发给每个
  session;`Server.cpp` 读 `FLOWMQ_QUEUE_DROP=newest|oldest` 设置(非法值启动即报错)。
- 每个 `Delivery` 带一个单调递增的 `deliverySequence`,使"最旧"跨
  `pendingDeliveries`/`blockedDeliveries` 两个容器可比较。`dropOldestQueued()`
  比较两个队列的 front,弹出全局最旧者;`pendingDeliveries` 保持发送顺序(§4.6
  的重传优先语义不破)。
- **关键正确性**:从 `pendingDeliveries` 丢一个 QoS>0 消息时,它已认领 inflight
  slot,必须 `releaseInflightIfPresent()` 释放,否则 Receive Maximum 窗口泄漏。
  从 `blockedDeliveries` 丢无需释放(尚未认领 slot)。丢之前先循环腾位再追加新
  消息,保证"最新必留"。
- 回归测试(`SessionLifecycleTests`):DropOldest 保留最新 1000、DropNewest 保留最旧
  1000(默认回归)、QoS1 满队列驱逐不泄漏流控窗口(以 `receiveMaximum=队列上限`
  构造,若 slot 泄漏则新消息被迫进 blocked 队列,断言 blocked 为 0)。三个测试均
  已验证:禁用对应逻辑后即失败。

---

### 缺口 C — TCP 出站写背压(改动最大)

**问题**:慢读者由 `writeTimeoutMs`(默认 5s)写超时兜底,超时即**断连**。
中间没有"有界缓冲 + 背压感知写"的中间态:要么快速写成功,要么 5s 后整连接丢失。

**现状写路径**:`Session::flush()` → `Connection::sendPacket()` →
`transport->writeAll()`,全程持 `writeMutex` 同步阻塞写;`pendingDeliveries`
在 `flush()` 开始时一次性 swap 出来逐条写。即**没有跨调用的出站缓冲队列**——
每条消息都在一次 `flush()` 内同步写出,写失败即断连。

**方案(选项)**:
- **C1(现状)**:同步写 + 写超时断连。简单,已有。
- **C2(新,opt-in)**:引入**每连接出站字节缓冲**。`writeAll` 在 socket 不可写
  时把数据追加到有界缓冲(上限 `FLOWMQ_WRITE_BUFFER`),`poll(POLLOUT)` 就绪后
  续写;缓冲满时对上游施加背压(`flush()` 提前返回,`pendingDeliveries` 保留
  未写部分,Receive Maximum 窗口不释放);持续无进展超 `writeTimeoutMs` 仍断连。
  这样"读得慢"≠"已死",慢消费者不再因瞬时阻塞被踢。

**关键约束**:
- 必须保留 `writeMutex` 单线程写不变式,避免交错。
- 缓冲有界,否则只是把内存问题从别处搬过来。
- 断连判定仍需总时限,不能无限缓冲(C2 的背压是"延缓断连 + 减少误踢",不是
  永不回收)。
- 与 QoS 流控的交互:C2 下消息"已写入 socket 缓冲"≠"已送达客户端",故
  Receive Maximum 的 inflight slot 只能在 PUBACK/PUBCOMP 释放(现状如此),
  C2 不改变这一点——这是正确的。

**opt-in**:`FLOWMQ_WRITE_BUFFER=<bytes>`,默认 `0`(=关闭,即 C1 现状)。

**影响面**:`Transport.h`/`TLS.cpp`/`WebSocket.cpp`(写路径)、`Connection.cpp`
(写缓冲管理)、`Session.cpp`(flush 部分写)、`Server.cpp`(读 env)。**改动面最大,
风险最高**。

**我的倾向**:C 的价值(慢消费者不被误踢)真实,但复杂度和回归风险显著高于 A。
建议**最后做**,且先以最小实现(仅 TCP 明文 + TLS 两条,WebSocket 暂不)落地。

## 三、建议的实施顺序

1. **A(背压可观测性)** — 先做。零风险、立即有运维价值、是所有后续工作的
   度量基础(没有 dropped 计数,无法验证 B/C 的效果)。
2. **B(丢弃策略)** — 可选,且建议只实现"丢最旧未入 inflight"的克制变体,
   默认不变。需你定夺是否值得。
3. **C(TCP 写背压)** — 最后做。价值真实但改动最大,建议在 A 提供度量后再凭
   数据决定。

## 四、需要你决定的问题

1. A 中"把队列满日志从 debug 提升到 info"——是否接受这一可见性变更?还是保持
   debug、仅靠 `$SYS` 计数暴露?(我倾向:计数始终累计 + `$SYS` 暴露,日志保持
   debug 不刷屏,靠 `$SYS` 订阅者观测。)
2. B 是否要做?若做,采用"丢最旧未入 inflight"的克制变体,还是完整 B2?
3. C 是否要做?若做,是否限定仅 TCP(明文+TLS)先落地?
