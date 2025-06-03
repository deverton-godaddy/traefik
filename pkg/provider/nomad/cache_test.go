package nomad

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/hashicorp/nomad/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testFilter implements serviceFilter for testing
func testFilter(enabled bool) serviceFilter {
	return func(ctx context.Context, name string, tags []string) bool {
		return enabled
	}
}

// nameTagFilter implements serviceFilter for testing specific name/tag combinations
func nameTagFilter(allowedNames map[string]bool, allowedTags map[string]bool) serviceFilter {
	return func(ctx context.Context, name string, tags []string) bool {
		if !allowedNames[name] {
			return false
		}
		for _, tag := range tags {
			if allowedTags[tag] {
				return true
			}
		}
		return false
	}
}

func TestNewServiceCache(t *testing.T) {
	filter := testFilter(true)
	cache := newServiceCache(filter, &api.Client{}, false)

	assert.NotNil(t, cache)
	assert.NotNil(t, cache.items)
	assert.NotNil(t, cache.filter)
}

func TestServiceCache_RegisterAndDeregister(t *testing.T) {
	tests := []struct {
		desc     string
		service  *api.ServiceRegistration
		validate func(t *testing.T, cache *serviceCache)
	}{
		{
			desc: "register single service",
			service: &api.ServiceRegistration{
				ID:          "service1",
				ServiceName: "web",
				Namespace:   "default",
				Tags:        []string{"v1"},
			},
			validate: func(t *testing.T, cache *serviceCache) {
				services, ok := cache.items["web"]
				require.True(t, ok)
				assert.Len(t, services, 1)
				assert.Len(t, services["service1"], 1)
				assert.Equal(t, "web", services["service1"][0].ServiceName)
			},
		},
		{
			desc: "deregister existing service",
			service: &api.ServiceRegistration{
				ID:          "service1",
				ServiceName: "web",
				Namespace:   "default",
				Tags:        []string{"v1"},
			},
			validate: func(t *testing.T, cache *serviceCache) {
				cache.registerServiceInstance(cache.items["web"]["service1"][0])
				cache.deregisterServiceInstance(cache.items["web"]["service1"][0])
				_, ok := cache.items["web"]
				assert.False(t, ok)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			cache := newServiceCache(testFilter(true), &api.Client{}, false)
			cache.registerServiceInstance(test.service)
			test.validate(t, cache)
		})
	}
}

func TestServiceCache_ConcurrentAccess(t *testing.T) {
	cache := newServiceCache(testFilter(true), &api.Client{}, false)
	var wg sync.WaitGroup
	numGoroutines := 10

	// Create multiple services to register
	services := make([]*api.ServiceRegistration, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		services[i] = &api.ServiceRegistration{
			ID:          fmt.Sprintf("service%d", i),
			ServiceName: "web",
			Namespace:   "default",
			Tags:        []string{"v1"},
		}
	}

	// Test concurrent registration
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			cache.registerServiceInstance(services[idx])
		}(i)
	}
	wg.Wait()

	assert.Len(t, cache.items["web"], numGoroutines)

	// Test concurrent deregistration
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			cache.deregisterServiceInstance(services[idx])
		}(i)
	}
	wg.Wait()

	assert.Len(t, cache.items, 0)
}

func TestServiceCache_ListServices(t *testing.T) {
	filter := testFilter(true)
	cache := newServiceCache(filter, &api.Client{}, false)

	// Register multiple services
	services := []*api.ServiceRegistration{
		{
			ID:          "service1",
			ServiceName: "web",
			Namespace:   "default",
			Tags:        []string{"v1"},
		},
		{
			ID:          "service2",
			ServiceName: "api",
			Namespace:   "default",
			Tags:        []string{"v2"},
		},
	}

	for _, svc := range services {
		cache.registerServiceInstance(svc)
	}

	// Test listing services
	stubs, err := cache.listServices(context.Background())
	require.NoError(t, err)
	assert.Len(t, stubs, 2)

	// Test with filter disabled
	cache = newServiceCache(testFilter(false), &api.Client{}, false)
	for _, svc := range services {
		cache.registerServiceInstance(svc)
	}
	stubs, err = cache.listServices(context.Background())
	require.NoError(t, err)
	assert.Len(t, stubs, 0)
}

func TestServiceCache_FetchService(t *testing.T) {
	filter := testFilter(true)
	cache := newServiceCache(filter, &api.Client{}, false)

	// Register a service
	service := &api.ServiceRegistration{
		ID:          "service1",
		ServiceName: "web",
		Namespace:   "default",
		Tags:        []string{"v1"},
	}
	cache.registerServiceInstance(service)

	// Test fetching existing service
	services, err := cache.fetchService(context.Background(), "web")
	require.NoError(t, err)
	assert.Len(t, services, 1)
	assert.Equal(t, service, services[0])

	// Test fetching non-existent service
	services, err = cache.fetchService(context.Background(), "nonexistent")
	require.NoError(t, err)
	assert.Len(t, services, 0)

	// Test with filter disabled
	cache = newServiceCache(testFilter(false), &api.Client{}, false)
	cache.registerServiceInstance(service)
	services, err = cache.fetchService(context.Background(), "web")
	require.NoError(t, err)
	assert.Len(t, services, 0)
}

func TestServiceCache_EdgeCases(t *testing.T) {
	tests := []struct {
		desc     string
		setup    func(t *testing.T, cache *serviceCache)
		validate func(t *testing.T, cache *serviceCache)
	}{
		{
			desc: "deregister non-existent service",
			setup: func(t *testing.T, cache *serviceCache) {
				cache.deregisterServiceInstance(&api.ServiceRegistration{
					ID:          "nonexistent",
					ServiceName: "nonexistent",
				})
			},
			validate: func(t *testing.T, cache *serviceCache) {
				assert.Len(t, cache.items, 0)
			},
		},
		{
			desc: "register multiple instances of same service",
			setup: func(t *testing.T, cache *serviceCache) {
				for i := 0; i < 3; i++ {
					cache.registerServiceInstance(&api.ServiceRegistration{
						ID:          fmt.Sprintf("service%d", i),
						ServiceName: "web",
						Tags:        []string{"v1"},
					})
				}
			},
			validate: func(t *testing.T, cache *serviceCache) {
				assert.Len(t, cache.items["web"], 3)
			},
		},
		{
			desc: "deregister last instance of service",
			setup: func(t *testing.T, cache *serviceCache) {
				svc := &api.ServiceRegistration{
					ID:          "service1",
					ServiceName: "web",
				}
				cache.registerServiceInstance(svc)
				cache.deregisterServiceInstance(svc)
			},
			validate: func(t *testing.T, cache *serviceCache) {
				_, exists := cache.items["web"]
				assert.False(t, exists)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			cache := newServiceCache(testFilter(true), &api.Client{}, false)
			test.setup(t, cache)
			test.validate(t, cache)
		})
	}
}

func TestServiceCache_Filtering(t *testing.T) {
	tests := []struct {
		desc          string
		filter        serviceFilter
		services      []*api.ServiceRegistration
		expectedCount int
		expectedNames []string
	}{
		{
			desc: "filter by service name",
			filter: nameTagFilter(
				map[string]bool{"web": true, "api": false},
				map[string]bool{"v1": true, "v2": true},
			),
			services: []*api.ServiceRegistration{
				{
					ID:          "web1",
					ServiceName: "web",
					Tags:        []string{"v1"},
				},
				{
					ID:          "api1",
					ServiceName: "api",
					Tags:        []string{"v1"},
				},
			},
			expectedCount: 1,
			expectedNames: []string{"web"},
		},
		{
			desc: "filter by tags",
			filter: nameTagFilter(
				map[string]bool{"web": true, "api": true},
				map[string]bool{"v1": true, "v2": false},
			),
			services: []*api.ServiceRegistration{
				{
					ID:          "web1",
					ServiceName: "web",
					Tags:        []string{"v1"},
				},
				{
					ID:          "web2",
					ServiceName: "web",
					Tags:        []string{"v2"},
				},
				{
					ID:          "api1",
					ServiceName: "api",
					Tags:        []string{"v1"},
				},
			},
			expectedCount: 2,
			expectedNames: []string{"web", "api"},
		},
		{
			desc: "filter by both name and tags",
			filter: nameTagFilter(
				map[string]bool{"web": true, "api": false},
				map[string]bool{"v1": true, "v2": false},
			),
			services: []*api.ServiceRegistration{
				{
					ID:          "web1",
					ServiceName: "web",
					Tags:        []string{"v1"},
				},
				{
					ID:          "web2",
					ServiceName: "web",
					Tags:        []string{"v2"},
				},
				{
					ID:          "api1",
					ServiceName: "api",
					Tags:        []string{"v1"},
				},
			},
			expectedCount: 1,
			expectedNames: []string{"web"},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			cache := newServiceCache(test.filter, &api.Client{}, false)

			// Register all services
			for _, svc := range test.services {
				cache.registerServiceInstance(svc)
			}

			// Test listServices
			stubs, err := cache.listServices(context.Background())
			require.NoError(t, err)
			assert.Len(t, stubs, test.expectedCount)

			// Verify service names
			names := make(map[string]bool)
			for _, stub := range stubs {
				for _, service := range stub.Services {
					names[service.ServiceName] = true
				}
			}
			assert.Equal(t, len(test.expectedNames), len(names))
			for _, name := range test.expectedNames {
				assert.True(t, names[name], "expected service %s to be present", name)
			}

			// Test fetchService for each service
			for _, svc := range test.services {
				services, err := cache.fetchService(context.Background(), svc.ServiceName)
				require.NoError(t, err)

				shouldBePresent := false
				for _, name := range test.expectedNames {
					if name == svc.ServiceName {
						shouldBePresent = true
						break
					}
				}

				if shouldBePresent {
					assert.NotEmpty(t, services, "expected service %s to be present", svc.ServiceName)
				} else {
					assert.Empty(t, services, "expected service %s to be filtered out", svc.ServiceName)
				}
			}
		})
	}
}
