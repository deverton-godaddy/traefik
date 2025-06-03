package nomad

import (
	"context"
	"sync"

	"github.com/hashicorp/nomad/api"
	"github.com/rs/zerolog/log"
	"github.com/traefik/traefik/v3/pkg/logs"
)

// serviceProvider defines the interface for service operations
type serviceFilter func(ctx context.Context, name string, tags []string) bool

type serviceCache struct {
	items  map[string]map[string][]*api.ServiceRegistration
	mutex  sync.RWMutex
	filter serviceFilter
	client *api.Client
	stale  bool
}

func newServiceCache(filter serviceFilter, client *api.Client, stale bool) *serviceCache {
	return &serviceCache{
		items:  make(map[string]map[string][]*api.ServiceRegistration),
		mutex:  sync.RWMutex{},
		filter: filter,
		client: client,
		stale:  stale,
	}
}

func (c *serviceCache) run(ctx context.Context, lastIndex uint64, serviceEventsChan chan<- *api.Events) error {
	logger := log.Ctx(ctx).With().Str(logs.ProviderName, "nomad").Logger()

	// start the event stream to listen for service changes
	eventCh, err := c.client.EventStream().Stream(ctx,
		map[api.Topic][]string{
			api.TopicService: {"*"},
		},
		lastIndex,
		(&api.QueryOptions{AllowStale: c.stale}).WithContext(ctx),
	)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			logger.Debug().Msg("stopping service cache")
			return ctx.Err()
		case events, ok := <-eventCh:
			if !ok {
				logger.Debug().Msg("service event stream closed")
				return nil
			}

			if events.Err != nil {
				logger.Err(events.Err).Msg("error in service event stream")
				return err
			}

			err := c.handleServiceEvents(ctx, events.Events)
			if err != nil {
				logger.Err(err).Msg("error handling service events")
				return err
			}

			select {
			case serviceEventsChan <- events:
			default:
				// drop event as the channel is full
			}
		}
	}
}

func (c *serviceCache) initCache(ctx context.Context) (uint64, error) {
	// populate the cache with existing services
	stubs, meta, err := c.client.Services().List((&api.QueryOptions{AllowStale: c.stale}).WithContext(ctx))
	if err != nil {
		return 0, err
	}

	for _, stub := range stubs {
		for _, serviceStub := range stub.Services {
			logger := log.Ctx(ctx).With().Str("serviceName", serviceStub.ServiceName).Logger()

			if !c.filter(ctx, serviceStub.ServiceName, serviceStub.Tags) {
				continue
			}

			serviceRegistrations, _, err := c.client.Services().Get(serviceStub.ServiceName, (&api.QueryOptions{AllowStale: c.stale}).WithContext(ctx))
			if err != nil {
				logger.Err(err).Str("service_name", serviceStub.ServiceName).Msg("failed to get service details")
				continue
			}

			for _, serviceRegistration := range serviceRegistrations {
				c.registerServiceInstance(serviceRegistration)
			}
		}
	}

	log.Ctx(ctx).Info().Msgf("service cache initialized with %d services", len(stubs))

	return meta.LastIndex, nil
}

func (c *serviceCache) handleServiceEvents(ctx context.Context, events []api.Event) error {
	for _, event := range events {
		serviceRegistration, err := event.Service()
		if err != nil {
			log.Warn().Err(err).Msg("failed to get service from event")
			continue
		}

		if serviceRegistration == nil {
			log.Warn().Msg("received nil service from event")
			continue
		}

		logger := log.Ctx(ctx).With().Str("serviceName", serviceRegistration.ServiceName).Logger()

		logger.Debug().Msgf("received service event: %s", event.Type)

		switch event.Type {
		case "ServiceRegistration":
			c.registerServiceInstance(serviceRegistration)
		case "ServiceDeregistration":
			c.deregisterServiceInstance(serviceRegistration)
		default:
			log.Warn().Str("event_type", event.Type).Msg("unhandled service event type")
		}
	}

	return nil
}

func (c *serviceCache) registerServiceInstance(service *api.ServiceRegistration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, ok := c.items[service.ServiceName]; !ok {
		c.items[service.ServiceName] = make(map[string][]*api.ServiceRegistration)
	}
	c.items[service.ServiceName][service.ID] = []*api.ServiceRegistration{service}
}

func (c *serviceCache) deregisterServiceInstance(service *api.ServiceRegistration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if services, ok := c.items[service.ServiceName]; ok {
		delete(services, service.ID)
		if len(services) == 0 {
			delete(c.items, service.ServiceName)
		}
	}
}

func (c *serviceCache) listServices(ctx context.Context) ([]*api.ServiceRegistrationListStub, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	stubs := make([]*api.ServiceRegistrationListStub, 0)
	for _, service := range c.items {
		for _, regs := range service {

			if !c.filter(ctx, regs[0].ServiceName, regs[0].Tags) {
				continue
			}

			services := make([]*api.ServiceRegistrationStub, 0)
			for _, reg := range regs {
				services = append(services, &api.ServiceRegistrationStub{
					ServiceName: reg.ServiceName,
					Tags:        reg.Tags,
				})
			}
			stubs = append(stubs, &api.ServiceRegistrationListStub{
				Namespace: regs[0].Namespace,
				Services:  services,
			})
		}
	}

	return stubs, nil
}

func (c *serviceCache) fetchService(ctx context.Context, name string) ([]*api.ServiceRegistration, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	result := make([]*api.ServiceRegistration, 0)

	if services, ok := c.items[name]; ok {
		for _, regs := range services {

			if !c.filter(ctx, regs[0].ServiceName, regs[0].Tags) {
				continue
			}

			result = append(result, regs...)
		}
		return result, nil
	}

	return []*api.ServiceRegistration{}, nil
}
