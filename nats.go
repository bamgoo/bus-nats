package bus_nats

import (
	"errors"
	"sync"
	"time"

	"github.com/bamgoo/bamgoo"
	"github.com/bamgoo/bus"
	"github.com/nats-io/nats.go"
)

var (
	errNatsInvalidConnection = errors.New("invalid nats connection")
	errNatsAlreadyRunning    = errors.New("nats bus is already running")
	errNatsNotRunning        = errors.New("nats bus is not running")
)

type (
	natsBusDriver struct{}

	natsBusConnection struct {
		mutex   sync.RWMutex
		running bool

		instance *bus.Instance
		setting  natsBusSetting
		client   *nats.Conn

		subjects map[string]struct{}
		subs     []*nats.Subscription

		stats map[string]*statsEntry
	}

	natsBusSetting struct {
		URL        string
		Token      string
		Username   string
		Password   string
		QueueGroup string
		Version    string
	}

	statsEntry struct {
		name         string
		numRequests  int
		numErrors    int
		totalLatency int64
	}
)

func init() {
	bamgoo.Register("nats", &natsBusDriver{})
}

func (driver *natsBusDriver) Connect(inst *bus.Instance) (bus.Connection, error) {
	setting := natsBusSetting{
		URL:     nats.DefaultURL,
		Version: "1.0.0",
	}

	if v, ok := inst.Config.Setting["url"].(string); ok && v != "" {
		setting.URL = v
	}
	if v, ok := inst.Config.Setting["server"].(string); ok && v != "" {
		setting.URL = v
	}
	if v, ok := inst.Config.Setting["token"].(string); ok && v != "" {
		setting.Token = v
	}
	if v, ok := inst.Config.Setting["user"].(string); ok && v != "" {
		setting.Username = v
	}
	if v, ok := inst.Config.Setting["username"].(string); ok && v != "" {
		setting.Username = v
	}
	if v, ok := inst.Config.Setting["pass"].(string); ok && v != "" {
		setting.Password = v
	}
	if v, ok := inst.Config.Setting["password"].(string); ok && v != "" {
		setting.Password = v
	}
	if v, ok := inst.Config.Setting["group"].(string); ok && v != "" {
		setting.QueueGroup = v
	}
	if v, ok := inst.Config.Setting["version"].(string); ok && v != "" {
		setting.Version = v
	}

	return &natsBusConnection{
		instance: inst,
		setting:  setting,
		subjects: make(map[string]struct{}, 0),
		subs:     make([]*nats.Subscription, 0),
		stats:    make(map[string]*statsEntry, 0),
	}, nil
}

func (c *natsBusConnection) Register(subject string) error {
	c.mutex.Lock()
	c.subjects[subject] = struct{}{}
	c.mutex.Unlock()
	return nil
}

func (c *natsBusConnection) Open() error {
	opts := []nats.Option{}
	if c.setting.Token != "" {
		opts = append(opts, nats.Token(c.setting.Token))
	}
	if c.setting.Username != "" || c.setting.Password != "" {
		opts = append(opts, nats.UserInfo(c.setting.Username, c.setting.Password))
	}

	client, err := nats.Connect(c.setting.URL, opts...)
	if err != nil {
		return err
	}
	c.client = client
	return nil
}

func (c *natsBusConnection) Close() error {
	if c.client != nil {
		c.client.Close()
	}
	return nil
}

func (c *natsBusConnection) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.running {
		return errNatsAlreadyRunning
	}
	if c.client == nil {
		return errNatsInvalidConnection
	}

	for subject := range c.subjects {
		callSubject := "call." + subject
		queueSubject := "queue." + subject
		eventSubject := "event." + subject

		callSub, err := c.client.QueueSubscribe(callSubject, c.queueGroup(callSubject), func(msg *nats.Msg) {
			started := time.Now()
			resp, callErr := c.handleCall(msg.Data)
			if callErr != nil {
				c.recordStats(subject, time.Since(started), callErr)
				return
			}
			_ = msg.Respond(resp)
			c.recordStats(subject, time.Since(started), nil)
		})
		if err != nil {
			return err
		}
		c.subs = append(c.subs, callSub)

		queueSub, err := c.client.QueueSubscribe(queueSubject, c.queueGroup(queueSubject), func(msg *nats.Msg) {
			started := time.Now()
			asyncErr := c.handleAsync(msg.Data)
			c.recordStats(subject, time.Since(started), asyncErr)
		})
		if err != nil {
			return err
		}
		c.subs = append(c.subs, queueSub)

		eventSub, err := c.client.Subscribe(eventSubject, func(msg *nats.Msg) {
			started := time.Now()
			asyncErr := c.handleAsync(msg.Data)
			c.recordStats(subject, time.Since(started), asyncErr)
		})
		if err != nil {
			return err
		}
		c.subs = append(c.subs, eventSub)
	}

	c.running = true
	return nil
}

func (c *natsBusConnection) Stop() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.running {
		return errNatsNotRunning
	}

	for _, sub := range c.subs {
		_ = sub.Unsubscribe()
	}
	c.subs = nil
	c.running = false
	return nil
}

func (c *natsBusConnection) Request(subject string, data []byte, timeout time.Duration) ([]byte, error) {
	if c.client == nil {
		return nil, errNatsInvalidConnection
	}

	msg, err := c.client.Request(subject, data, timeout)
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}

func (c *natsBusConnection) Publish(subject string, data []byte) error {
	if c.client == nil {
		return errNatsInvalidConnection
	}
	return c.client.Publish(subject, data)
}

func (c *natsBusConnection) Enqueue(subject string, data []byte) error {
	if c.client == nil {
		return errNatsInvalidConnection
	}
	return c.client.Publish(subject, data)
}

func (c *natsBusConnection) Stats() []bamgoo.ServiceStats {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	all := make([]bamgoo.ServiceStats, 0, len(c.stats))
	for _, st := range c.stats {
		avg := int64(0)
		if st.numRequests > 0 {
			avg = st.totalLatency / int64(st.numRequests)
		}
		all = append(all, bamgoo.ServiceStats{
			Name:         st.name,
			Version:      c.setting.Version,
			NumRequests:  st.numRequests,
			NumErrors:    st.numErrors,
			TotalLatency: st.totalLatency,
			AvgLatency:   avg,
		})
	}
	return all
}

func (c *natsBusConnection) queueGroup(subject string) string {
	if c.setting.QueueGroup != "" {
		return c.setting.QueueGroup + "." + subject
	}
	return subject
}

func (c *natsBusConnection) handleCall(data []byte) ([]byte, error) {
	if c.instance == nil {
		c.instance = &bus.Instance{}
	}
	return c.instance.HandleCall(data)
}

func (c *natsBusConnection) handleAsync(data []byte) error {
	if c.instance == nil {
		c.instance = &bus.Instance{}
	}
	return c.instance.HandleAsync(data)
}

func (c *natsBusConnection) recordStats(subject string, cost time.Duration, err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	st, ok := c.stats[subject]
	if !ok {
		st = &statsEntry{name: subject}
		c.stats[subject] = st
	}
	st.numRequests++
	st.totalLatency += cost.Milliseconds()
	if err != nil {
		st.numErrors++
	}
}
