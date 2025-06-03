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

// extraConfFromTags defines a function type that takes a slice of tags and returns a configuration
type extraConfFromTags func(tags []string) configuration

type eventClientAPI interface {
	Stream(ctx context.Context, topics map[api.Topic][]string, index uint64, q *api.QueryOptions) (<-chan *api.Events, error)
}

type serviceClientAPI interface {
	List(q *api.QueryOptions) ([]*api.ServiceRegistrationListStub, *api.QueryMeta, error)
	Get(name string, q *api.QueryOptions) ([]*api.ServiceRegistration, *api.QueryMeta, error)
}

// serviceCacheEntry holds both the service registration and its corresponding item
type serviceCacheEntry struct {
	registrations []*api.ServiceRegistration
	items         []item
}

type serviceCache struct {
	filter    serviceFilter
	extraConf extraConfFromTags
	stale     bool

	// cache holds the service instances indexed by service name
	cache map[string]serviceCacheEntry
	mutex sync.RWMutex

	eventClient   eventClientAPI
	serviceClient serviceClientAPI
}

func newServiceCache(filter serviceFilter, extraConf extraConfFromTags, eventClient eventClientAPI, serviceClient serviceClientAPI, stale bool) *serviceCache {
	return &serviceCache{
		cache:         make(map[string]serviceCacheEntry),
		mutex:         sync.RWMutex{},
		filter:        filter,
		extraConf:     extraConf,
		eventClient:   eventClient,
		serviceClient: serviceClient,
		stale:         stale,
	}
}

func (c *serviceCache) run(ctx context.Context, lastIndex uint64, serviceEventsChan chan<- *api.Events) error {
	logger := log.Ctx(ctx).With().Str(logs.ProviderName, "nomad").Logger()

	// start the event stream to listen for service changes
	eventCh, err := c.eventClient.Stream(ctx,
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
				return events.Err
			}

			if events.IsHeartbeat() {
				continue
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
	stubs, meta, err := c.serviceClient.List((&api.QueryOptions{AllowStale: c.stale}).WithContext(ctx))
	if err != nil {
		return 0, err
	}

	for _, stub := range stubs {
		for _, serviceStub := range stub.Services {
			logger := log.Ctx(ctx).With().Str("serviceName", serviceStub.ServiceName).Logger()

			if !c.filter(ctx, serviceStub.ServiceName, serviceStub.Tags) {
				continue
			}

			serviceRegistrations, _, err := c.serviceClient.Get(serviceStub.ServiceName, (&api.QueryOptions{AllowStale: c.stale}).WithContext(ctx))
			if err != nil {
				logger.Err(err).Str("service_name", serviceStub.ServiceName).Msg("failed to get service details")
				continue
			}

			for _, serviceRegistration := range serviceRegistrations {
				c.registerServiceInstance(ctx, serviceRegistration)
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
			c.registerServiceInstance(ctx, serviceRegistration)
		case "ServiceDeregistration":
			c.deregisterServiceInstance(ctx, serviceRegistration)
		default:
			log.Warn().Str("event_type", event.Type).Msg("unhandled service event type")
		}
	}

	return nil
}

func (c *serviceCache) registerServiceInstance(ctx context.Context, serviceRegistration *api.ServiceRegistration) {
	serviceName := serviceRegistration.ServiceName

	// If the service doesn't pass the filter, we may need to delete it
	if !c.filter(ctx, serviceName, serviceRegistration.Tags) {
		c.deregisterServiceInstance(ctx, serviceRegistration)
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	cacheEntry, ok := c.cache[serviceName]

	if ok {

		// Services generally have few instances, so we can afford to iterate through them i.e. O(n) complexity
		// If we find this is too slow in practice, we could create a secondary index by ID to index and bring this to O(1) at the cost
		// of additional memory usage and code complexity.
		for i, instance := range cacheEntry.registrations {
			// If we find an existing instance with the same ID, we update it
			if instance.ID == serviceRegistration.ID {
				cacheEntry.registrations[i] = serviceRegistration
				cacheEntry.items[i] = c.newItem(serviceRegistration)
				c.cache[serviceName] = cacheEntry
				return
			}
		}

		cacheEntry.registrations = append(cacheEntry.registrations, serviceRegistration)
		cacheEntry.items = append(cacheEntry.items, c.newItem(serviceRegistration))

		c.cache[serviceName] = cacheEntry
	} else {
		// Pre-allocate with capacity for potential future instances
		cacheEntry = serviceCacheEntry{
			registrations: make([]*api.ServiceRegistration, 1, 4),
			items:         make([]item, 1, 4),
		}
		cacheEntry.registrations[0] = serviceRegistration
		cacheEntry.items[0] = c.newItem(serviceRegistration)
		c.cache[serviceName] = cacheEntry
	}
}

func (c *serviceCache) deregisterServiceInstance(_ context.Context, serviceRegistration *api.ServiceRegistration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	serviceName := serviceRegistration.ServiceName

	cacheEntry, ok := c.cache[serviceName]
	if !ok {
		return // Service doesn't exist
	}

	// Find the index first, then do the swap-and-truncate in a separate step
	idx := -1
	for i, instance := range cacheEntry.registrations {
		if instance.ID == serviceRegistration.ID {
			idx = i
			break
		}
	}

	if idx >= 0 {
		// No need to check length since we found an item
		lastIdx := len(cacheEntry.registrations) - 1
		if idx < lastIdx {
			// Swap both registrations and items
			cacheEntry.registrations[idx] = cacheEntry.registrations[lastIdx]
			cacheEntry.items[idx] = cacheEntry.items[lastIdx]
		}
		// Truncate both slices
		cacheEntry.registrations = cacheEntry.registrations[:lastIdx]
		cacheEntry.items = cacheEntry.items[:lastIdx]

		// Update the map or clean up if the service is now empty
		if len(cacheEntry.registrations) > 0 {
			c.cache[serviceName] = cacheEntry
		} else {
			delete(c.cache, serviceName)
		}
	}
}

func (c *serviceCache) listServices(_ context.Context) ([]*api.ServiceRegistrationListStub, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	// Pre-allocate the result slice with the exact capacity needed
	services := make([]*api.ServiceRegistrationListStub, 0, len(c.cache))

	for _, cacheEntry := range c.cache {
		// Pre-allocate the stubs slice with exact capacity needed
		servicesStubs := make([]*api.ServiceRegistrationStub, len(cacheEntry.registrations))

		// Fill in servicesStubs directly with indexing instead of append
		for i, instance := range cacheEntry.registrations {
			servicesStubs[i] = &api.ServiceRegistrationStub{
				ServiceName: instance.ServiceName,
				Tags:        instance.Tags,
			}
		}

		services = append(services, &api.ServiceRegistrationListStub{
			Namespace: cacheEntry.registrations[0].Namespace,
			Services:  servicesStubs,
		})
	}

	return services, nil
}

// fetchService retrieves service registrations by name from the cache
// The caller is expected to not mutate the returned slice
func (c *serviceCache) fetchService(_ context.Context, name string) ([]*api.ServiceRegistration, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if entry, exists := c.cache[name]; exists {
		return entry.registrations, nil
	}

	return []*api.ServiceRegistration{}, nil
}

func (c *serviceCache) getNomadServiceData(_ context.Context) ([]item, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	// Count total number of service instances to pre-allocate the slice
	totalInstances := 0
	for _, entry := range c.cache {
		totalInstances += len(entry.registrations)
	}

	// Pre-allocate the slice with the exact capacity needed
	items := make([]item, 0, totalInstances)

	// Use the cached items directly
	for _, entry := range c.cache {
		items = append(items, entry.items...)
	}

	return items, nil
}

// newItem creates a new serviceInstance from a service registration
func (c *serviceCache) newItem(serviceRegistration *api.ServiceRegistration) item {
	return item{
		ID:         serviceRegistration.ID,
		Name:       serviceRegistration.ServiceName,
		Namespace:  serviceRegistration.Namespace,
		Node:       serviceRegistration.NodeID,
		Datacenter: serviceRegistration.Datacenter,
		Address:    serviceRegistration.Address,
		Port:       serviceRegistration.Port,
		Tags:       serviceRegistration.Tags,
		ExtraConf:  c.extraConf(serviceRegistration.Tags),
	}
}
