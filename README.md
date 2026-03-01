# bus-nats

`bus-nats` 是 `bus` 模块的 `nats` 驱动。

## 安装

```bash
go get github.com/infrago/bus@latest
go get github.com/infrago/bus-nats@latest
```

## 接入

```go
import (
    _ "github.com/infrago/bus"
    _ "github.com/infrago/bus-nats"
    "github.com/infrago/infra"
)

func main() {
    infra.Run()
}
```

## 配置示例

```toml
[bus]
driver = "nats"
```

## 公开 API（摘自源码）

- `func (driver *natsBusDriver) Connect(inst *bus.Instance) (bus.Connection, error)`
- `func (c *natsBusConnection) Register(subject string) error`
- `func (c *natsBusConnection) Open() error`
- `func (c *natsBusConnection) Close() error`
- `func (c *natsBusConnection) Start() error`
- `func (c *natsBusConnection) Stop() error`
- `func (c *natsBusConnection) Request(subject string, data []byte, timeout time.Duration) ([]byte, error)`
- `func (c *natsBusConnection) Publish(subject string, data []byte) error`
- `func (c *natsBusConnection) Enqueue(subject string, data []byte) error`
- `func (c *natsBusConnection) Stats() []infra.ServiceStats`
- `func (c *natsBusConnection) ListNodes() []infra.NodeInfo`
- `func (c *natsBusConnection) ListServices() []infra.ServiceInfo`

## 排错

- driver 未生效：确认模块段 `driver` 值与驱动名一致
- 连接失败：检查 endpoint/host/port/鉴权配置
