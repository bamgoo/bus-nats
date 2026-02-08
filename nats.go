package bus_nats

import (
	"errors"
	"sync"
	"time"

	"github.com/bamgoo/bamgoo"
	"github.com/bamgoo/bus"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
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
		services []micro.Service

		subjects map[string]struct{}
		subs     []*nats.Subscription
	}

	natsBusSetting struct {
		URL        string
		Username   string
		Password   string
		QueueGroup string
		Version    string
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

	if v, ok := inst.Config.Setting["url"].(string); ok {
		setting.URL = v
	}
	if v, ok := inst.Config.Setting["server"].(string); ok {
		setting.URL = v
	}
	if v, ok := inst.Config.Setting["user"].(string); ok {
		setting.Username = v
	}
	if v, ok := inst.Config.Setting["username"].(string); ok {
		setting.Username = v
	}
	if v, ok := inst.Config.Setting["pass"].(string); ok {
		setting.Password = v
	}
	if v, ok := inst.Config.Setting["password"].(string); ok {
		setting.Password = v
	}
	if v, ok := inst.Config.Setting["group"].(string); ok {
		setting.QueueGroup = v
	}
	if v, ok := inst.Config.Setting["version"].(string); ok {
		setting.Version = v
	}

	return &natsBusConnection{
		instance: inst,
		setting:  setting,
		subjects: make(map[string]struct{}, 0),
		subs:     make([]*nats.Subscription, 0),
		services: make([]micro.Service, 0),
	}, nil
}

// Register registers a service subject.
func (c *natsBusConnection) Register(subject string) error {
	c.mutex.Lock()
	c.subjects[subject] = struct{}{}
	c.mutex.Unlock()
	return nil
}

func (c *natsBusConnection) Open() error {
	opts := []nats.Option{}
	if c.setting.Username != "" && c.setting.Password != "" {
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

	// Create micro services for each registered subject
	for subject := range c.subjects {
		// Create micro service for call (request/reply)
		svc, err := micro.AddService(c.client, micro.Config{
			Name:        subject,
			Version:     c.setting.Version,
			Description: "Bamgoo service: " + subject,
			QueueGroup:  c.queueGroup(subject),
			Endpoint: &micro.EndpointConfig{
				Subject: "call." + subject,
				Handler: micro.HandlerFunc(c.handleMicroRequest),
			},
		})
		if err != nil {
			return err
		}
		c.services = append(c.services, svc)

		// queue - async with queue group (one subscriber receives)
		queueSub := "queue." + subject
		sub, err := c.client.QueueSubscribe(queueSub, c.queueGroup(queueSub), func(msg *nats.Msg) {
			c.handleRequest(msg.Data)
		})
		if err != nil {
			return err
		}
		c.subs = append(c.subs, sub)

		// event - broadcast to all subscribers
		eventSub := "event." + subject
		sub, err = c.client.Subscribe(eventSub, func(msg *nats.Msg) {
			c.handleRequest(msg.Data)
		})
		if err != nil {
			return err
		}
		c.subs = append(c.subs, sub)
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

	// Stop micro services
	for _, svc := range c.services {
		_ = svc.Stop()
	}
	c.services = nil

	// Unsubscribe from queue and event
	for _, sub := range c.subs {
		_ = sub.Unsubscribe()
	}
	c.subs = nil

	c.running = false
	return nil
}

// Request sends a synchronous request and waits for reply.
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

// Publish broadcasts an event to all subscribers.
func (c *natsBusConnection) Publish(subject string, data []byte) error {
	if c.client == nil {
		return errNatsInvalidConnection
	}
	return c.client.Publish(subject, data)
}

// Enqueue publishes to a queue (one of the subscribers will receive).
func (c *natsBusConnection) Enqueue(subject string, data []byte) error {
	if c.client == nil {
		return errNatsInvalidConnection
	}
	return c.client.Publish(subject, data)
}

// Stats returns statistics for all micro services.
func (c *natsBusConnection) Stats() []bamgoo.ServiceStats {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	stats := make([]bamgoo.ServiceStats, 0, len(c.services))
	for _, svc := range c.services {
		info := svc.Info()
		st := svc.Stats()

		var numRequests, numErrors int
		var totalLatency int64
		for _, ep := range st.Endpoints {
			numRequests += ep.NumRequests
			numErrors += ep.NumErrors
			totalLatency += int64(ep.ProcessingTime.Milliseconds())
		}

		avgLatency := int64(0)
		if numRequests > 0 {
			avgLatency = totalLatency / int64(numRequests)
		}

		stats = append(stats, bamgoo.ServiceStats{
			Name:         info.Name,
			Version:      info.Version,
			NumRequests:  numRequests,
			NumErrors:    numErrors,
			TotalLatency: totalLatency,
			AvgLatency:   avgLatency,
		})
	}
	return stats
}

func (c *natsBusConnection) queueGroup(subject string) string {
	if c.setting.QueueGroup != "" {
		return c.setting.QueueGroup
	}
	return subject
}

// handleMicroRequest handles micro service requests.
func (c *natsBusConnection) handleMicroRequest(req micro.Request) {
	resp, err := c.handleRequest(req.Data())
	if err != nil {
		req.Error("500", err.Error(), nil)
		return
	}
	req.Respond(resp)
}

func (c *natsBusConnection) handleRequest(data []byte) ([]byte, error) {
	if c.instance == nil {
		c.instance = &bus.Instance{}
	}
	return c.instance.HandleCall(data)
}
