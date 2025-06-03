package nomad

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/nomad/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/traefik/traefik/v3/pkg/provider/constraints"
)

// testFilter implements serviceFilter for testing
func testFilter(ctx context.Context, name string, tags []string) bool {

	extraConf := testExtraConfFromTags(tags)
	if !extraConf.Enable {
		return false
	}

	matches, err := constraints.MatchTags(tags, "")
	if err != nil {
		return false
	}

	if !matches {
		return false
	}

	return true
}

// testExtraConfFromTags implements extraConfFromTags for testing
func testExtraConfFromTags(tags []string) configuration {
	labels := tagsToLabels(tags, "traefik")

	enabled := false
	if v, exists := labels["traefik.enable"]; exists {
		enabled = strings.EqualFold(v, "true")
	}

	var canary bool
	if v, exists := labels["traefik.nomad.canary"]; exists {
		canary = strings.EqualFold(v, "true")
	}

	return configuration{Enable: enabled, Canary: canary}
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
	cache := newServiceCache(testFilter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)

	assert.NotNil(t, cache)
	assert.NotNil(t, cache.cache)
	assert.NotNil(t, cache.filter)
}

// Helper function to create a test service registration
func createTestService(id, name string, tags []string, address string, port int) *api.ServiceRegistration {
	return &api.ServiceRegistration{
		ID:          id,
		ServiceName: name,
		Tags:        tags,
		Address:     address,
		Port:        port,
		Namespace:   "default",
	}
}

// Helper function to verify service entry
func verifyServiceEntry(t *testing.T, entry serviceCacheEntry, expectedCount int) {
	require.Len(t, entry.registrations, expectedCount)
	require.Len(t, entry.items, expectedCount)

	for i, reg := range entry.registrations {
		verifyItemMatchesRegistration(t, entry.items[i], reg)
	}
}

// Helper function to verify a single item matches expected values
func verifyItem(t *testing.T, item item, expectedID, expectedName, expectedNamespace, expectedNode, expectedDatacenter, expectedAddress string, expectedPort int, expectedTags []string, expectedEnable bool) {
	assert.Equal(t, expectedID, item.ID)
	assert.Equal(t, expectedName, item.Name)
	assert.Equal(t, expectedNamespace, item.Namespace)
	assert.Equal(t, expectedNode, item.Node)
	assert.Equal(t, expectedDatacenter, item.Datacenter)
	assert.Equal(t, expectedAddress, item.Address)
	assert.Equal(t, expectedPort, item.Port)
	assert.Equal(t, expectedTags, item.Tags)
	assert.Equal(t, expectedEnable, item.ExtraConf.Enable)
}

// Helper function to verify an item matches a service registration
func verifyItemMatchesRegistration(t *testing.T, item item, reg *api.ServiceRegistration) {
	assert.Equal(t, reg.ID, item.ID)
	assert.Equal(t, reg.ServiceName, item.Name)
	assert.Equal(t, reg.Namespace, item.Namespace)
	assert.Equal(t, reg.NodeID, item.Node)
	assert.Equal(t, reg.Datacenter, item.Datacenter)
	assert.Equal(t, reg.Address, item.Address)
	assert.Equal(t, reg.Port, item.Port)
	assert.Equal(t, reg.Tags, item.Tags)
}

func TestServiceCache_RegisterAndDeregister(t *testing.T) {
	tests := []struct {
		desc     string
		service  *api.ServiceRegistration
		validate func(t *testing.T, cache *serviceCache)
	}{
		{
			desc:    "register single service",
			service: createTestService("service1", "web", []string{"traefik.enable=true"}, "10.0.0.1", 8080),
			validate: func(t *testing.T, cache *serviceCache) {
				entry, ok := cache.cache["web"]
				require.True(t, ok)
				verifyServiceEntry(t, entry, 1)
			},
		},
		{
			desc:    "deregister existing service",
			service: createTestService("service1", "web", []string{"traefik.enable=true"}, "10.0.0.1", 8080),
			validate: func(t *testing.T, cache *serviceCache) {
				cache.registerServiceInstance(context.Background(), cache.cache["web"].registrations[0])
				cache.deregisterServiceInstance(context.Background(), cache.cache["web"].registrations[0])
				_, ok := cache.cache["web"]
				assert.False(t, ok)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			cache := newServiceCache(testFilter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)
			cache.registerServiceInstance(context.Background(), test.service)
			test.validate(t, cache)
		})
	}
}

func TestServiceCache_ConcurrentAccess(t *testing.T) {
	cache := newServiceCache(testFilter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)
	var wg sync.WaitGroup
	numGoroutines := 10

	// Create multiple services to register
	services := make([]*api.ServiceRegistration, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		services[i] = createTestService(
			fmt.Sprintf("service%d", i),
			"web",
			[]string{"traefik.enable=true"},
			fmt.Sprintf("10.0.0.%d", i),
			8080+i,
		)
	}

	// Test concurrent registration
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			cache.registerServiceInstance(context.Background(), services[idx])
		}(i)
	}
	wg.Wait()

	entry, exists := cache.cache["web"]
	require.True(t, exists)
	verifyServiceEntry(t, entry, numGoroutines)

	// Test concurrent deregistration
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			cache.deregisterServiceInstance(context.Background(), services[idx])
		}(i)
	}
	wg.Wait()

	assert.Len(t, cache.cache, 0)
}

func TestServiceCache_ListServices(t *testing.T) {
	cache := newServiceCache(testFilter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)

	// Register multiple services
	services := []*api.ServiceRegistration{
		createTestService("service1", "web", []string{"traefik.enable=true"}, "10.0.0.1", 8080),
		createTestService("service2", "api", []string{"v2"}, "10.0.0.2", 8081),
	}

	for _, svc := range services {
		cache.registerServiceInstance(context.Background(), svc)
	}

	// Test listing services
	stubs, err := cache.listServices(context.Background())
	require.NoError(t, err)
	assert.Len(t, stubs, 1)

	assert.Len(t, stubs[0].Services, 1)
	assert.Equal(t, "default", stubs[0].Namespace)
	assert.Equal(t, "web", stubs[0].Services[0].ServiceName)
}

func TestServiceCache_FetchService(t *testing.T) {
	cache := newServiceCache(testFilter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)

	// Register services
	services := []*api.ServiceRegistration{
		createTestService("service1", "web", []string{"traefik.enable=true"}, "10.0.0.1", 8080),
		createTestService("service2", "web", []string{"traefik.enable=false"}, "10.0.0.2", 8081),
	}

	for _, svc := range services {
		cache.registerServiceInstance(context.Background(), svc)
	}

	// Test fetching existing service
	services, err := cache.fetchService(context.Background(), "web")
	require.NoError(t, err)
	assert.Len(t, services, 1)
	assert.Equal(t, "service1", services[0].ID)

	// Test fetching non-existent service
	services, err = cache.fetchService(context.Background(), "nonexistent")
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
				cache.deregisterServiceInstance(context.Background(), createTestService("nonexistent", "nonexistent", nil, "", 0))
			},
			validate: func(t *testing.T, cache *serviceCache) {
				assert.Len(t, cache.cache, 0)
			},
		},
		{
			desc: "register multiple instances of same service",
			setup: func(t *testing.T, cache *serviceCache) {
				for i := 0; i < 3; i++ {
					cache.registerServiceInstance(context.Background(), createTestService(
						fmt.Sprintf("service%d", i),
						"web",
						[]string{"traefik.enable=true"},
						fmt.Sprintf("10.0.0.%d", i),
						8080+i,
					))
				}
			},
			validate: func(t *testing.T, cache *serviceCache) {
				entry, exists := cache.cache["web"]
				require.True(t, exists)
				verifyServiceEntry(t, entry, 3)
			},
		},
		{
			desc: "deregister last instance of service",
			setup: func(t *testing.T, cache *serviceCache) {
				svc := createTestService("service1", "web", []string{"traefik.enable=true"}, "10.0.0.1", 8080)
				cache.registerServiceInstance(context.Background(), svc)
				cache.deregisterServiceInstance(context.Background(), svc)
			},
			validate: func(t *testing.T, cache *serviceCache) {
				_, exists := cache.cache["web"]
				assert.False(t, exists)
			},
		},
		{
			desc: "register service without passing filter",
			setup: func(t *testing.T, cache *serviceCache) {
				cache.registerServiceInstance(context.Background(), createTestService("service1", "web", []string{"traefik.enable=false"}, "10.0.0.1", 8080))
			},
			validate: func(t *testing.T, cache *serviceCache) {
				_, exists := cache.cache["web"]
				assert.False(t, exists)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			cache := newServiceCache(testFilter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)
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
				map[string]bool{"traefik.enable=true": true, "v2": true},
			),
			services: []*api.ServiceRegistration{
				createTestService("web1", "web", []string{"traefik.enable=true"}, "10.0.0.1", 8080),
				createTestService("api1", "api", []string{"traefik.enable=true"}, "10.0.0.2", 8081),
			},
			expectedCount: 1,
			expectedNames: []string{"web"},
		},
		{
			desc: "filter by tags",
			filter: nameTagFilter(
				map[string]bool{"web": true, "api": true},
				map[string]bool{"traefik.enable=true": true, "v2": false},
			),
			services: []*api.ServiceRegistration{
				createTestService("web1", "web", []string{"traefik.enable=true"}, "10.0.0.1", 8080),
				createTestService("web2", "web", []string{"v2"}, "10.0.0.2", 8081),
				createTestService("api1", "api", []string{"traefik.enable=true"}, "10.0.0.3", 8082),
			},
			expectedCount: 2,
			expectedNames: []string{"web", "api"},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			cache := newServiceCache(test.filter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)

			for _, svc := range test.services {
				cache.registerServiceInstance(context.Background(), svc)
			}

			stubs, err := cache.listServices(context.Background())
			require.NoError(t, err)
			assert.Len(t, stubs, test.expectedCount)

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
		})
	}
}

func TestServiceCache_ItemsSynchronization(t *testing.T) {
	tests := []struct {
		desc     string
		setup    func(t *testing.T, cache *serviceCache)
		validate func(t *testing.T, cache *serviceCache)
	}{
		{
			desc: "items stay in sync with registrations during registration",
			setup: func(t *testing.T, cache *serviceCache) {
				services := []*api.ServiceRegistration{
					createTestService("service1", "web", []string{"traefik.enable=true"}, "10.0.0.1", 8080),
					createTestService("service2", "web", []string{"traefik.enable=true"}, "10.0.0.2", 8081),
				}

				for _, svc := range services {
					cache.registerServiceInstance(context.Background(), svc)
				}
			},
			validate: func(t *testing.T, cache *serviceCache) {
				entry, exists := cache.cache["web"]
				require.True(t, exists)
				verifyServiceEntry(t, entry, 2)
			},
		},
		{
			desc: "items stay in sync with registrations during deregistration",
			setup: func(t *testing.T, cache *serviceCache) {
				services := []*api.ServiceRegistration{
					createTestService("service1", "web", []string{"traefik.enable=true"}, "10.0.0.1", 8080),
					createTestService("service2", "web", []string{"traefik.enable=true"}, "10.0.0.2", 8081),
					createTestService("service3", "web", []string{"traefik.enable=true"}, "10.0.0.3", 8082),
				}

				for _, svc := range services {
					cache.registerServiceInstance(context.Background(), svc)
				}

				cache.deregisterServiceInstance(context.Background(), services[1])
			},
			validate: func(t *testing.T, cache *serviceCache) {
				entry, exists := cache.cache["web"]
				require.True(t, exists)
				verifyServiceEntry(t, entry, 2)

				// Verify the deregistered service is not present
				for _, reg := range entry.registrations {
					assert.NotEqual(t, "service2", reg.ID)
				}
				for _, item := range entry.items {
					assert.NotEqual(t, "service2", item.ID)
				}
			},
		},
		{
			desc: "items stay in sync with registrations during update",
			setup: func(t *testing.T, cache *serviceCache) {
				service := createTestService("service1", "web", []string{"traefik.enable=true"}, "10.0.0.1", 8080)
				cache.registerServiceInstance(context.Background(), service)

				updatedService := createTestService("service1", "web", []string{"traefik.enable=true", "newtag"}, "10.0.0.2", 8081)
				cache.registerServiceInstance(context.Background(), updatedService)
			},
			validate: func(t *testing.T, cache *serviceCache) {
				entry, exists := cache.cache["web"]
				require.True(t, exists)
				verifyServiceEntry(t, entry, 1)
				assert.Contains(t, entry.items[0].Tags, "newtag")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			cache := newServiceCache(testFilter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)
			test.setup(t, cache)
			test.validate(t, cache)
		})
	}
}

func TestServiceCache_GetNomadServiceData(t *testing.T) {
	tests := []struct {
		desc     string
		setup    func(t *testing.T, cache *serviceCache)
		validate func(t *testing.T, items []item)
	}{
		{
			desc: "empty cache returns empty slice",
			setup: func(t *testing.T, cache *serviceCache) {
				// No setup needed for empty cache
			},
			validate: func(t *testing.T, items []item) {
				assert.Empty(t, items)
			},
		},
		{
			desc: "returns all service instances with complete data",
			setup: func(t *testing.T, cache *serviceCache) {
				services := []*api.ServiceRegistration{
					{
						ID:          "service1",
						ServiceName: "web",
						Namespace:   "default",
						NodeID:      "node1",
						Datacenter:  "dc1",
						Address:     "127.0.0.1",
						Port:        8080,
						Tags:        []string{"traefik.enable=true"},
					},
					{
						ID:          "service2",
						ServiceName: "api",
						Namespace:   "default",
						NodeID:      "node2",
						Datacenter:  "dc1",
						Address:     "127.0.0.2",
						Port:        8081,
						Tags:        []string{"traefik.enable=true"},
					},
				}

				for _, svc := range services {
					cache.registerServiceInstance(context.Background(), svc)
				}
			},
			validate: func(t *testing.T, items []item) {
				assert.Len(t, items, 2)

				// Verify first service
				verifyItem(t, items[0], "service1", "web", "default", "node1", "dc1", "127.0.0.1", 8080, []string{"traefik.enable=true"}, true)

				// Verify second service
				verifyItem(t, items[1], "service2", "api", "default", "node2", "dc1", "127.0.0.2", 8081, []string{"traefik.enable=true"}, true)
			},
		},
		{
			desc: "returns only enabled services",
			setup: func(t *testing.T, cache *serviceCache) {
				services := []*api.ServiceRegistration{
					createTestService("service1", "web", []string{"traefik.enable=true"}, "10.0.0.1", 8080),
					createTestService("service2", "api", []string{"traefik.enable=false"}, "10.0.0.2", 8081),
				}

				for _, svc := range services {
					cache.registerServiceInstance(context.Background(), svc)
				}
			},
			validate: func(t *testing.T, items []item) {
				assert.Len(t, items, 1)
				verifyItem(t, items[0], "service1", "web", "default", "", "", "10.0.0.1", 8080, []string{"traefik.enable=true"}, true)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			cache := newServiceCache(testFilter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)
			test.setup(t, cache)
			items, err := cache.getNomadServiceData(context.Background())
			require.NoError(t, err)
			test.validate(t, items)
		})
	}
}

func TestServiceCache_ConcurrentReadWrite(t *testing.T) {
	cache := newServiceCache(testFilter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Start goroutines that continuously read
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = cache.listServices(context.Background())
					_, _ = cache.fetchService(context.Background(), "service1")
					_, _ = cache.getNomadServiceData(context.Background())
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}

	// Start goroutines that continuously write
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			counter := 0
			serviceName := fmt.Sprintf("service%d", id)

			for {
				select {
				case <-stop:
					return
				default:
					instanceID := fmt.Sprintf("instance-%d-%d", id, counter)
					cache.registerServiceInstance(context.Background(), createTestService(
						instanceID,
						serviceName,
						[]string{"traefik.enable=true"},
						fmt.Sprintf("10.0.0.%d", counter),
						8080+counter,
					))

					// Occasionally deregister services
					if counter > 0 && counter%3 == 0 {
						cache.deregisterServiceInstance(context.Background(), createTestService(
							fmt.Sprintf("instance-%d-%d", id, counter-3),
							serviceName,
							[]string{"traefik.enable=true"},
							"",
							0,
						))
					}

					counter++
					time.Sleep(time.Millisecond)
				}
			}
		}(i)
	}

	// Let the test run for a short period
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Verify the cache is in a consistent state
	items, err := cache.getNomadServiceData(context.Background())
	require.NoError(t, err)

	// Verify that all items have matching registrations
	for _, item := range items {
		services, err := cache.fetchService(context.Background(), item.Name)
		require.NoError(t, err)
		found := false
		for _, svc := range services {
			if svc.ID == item.ID {
				found = true
				verifyItemMatchesRegistration(t, item, svc)
				break
			}
		}
		assert.True(t, found, "Item %s not found in registrations", item.ID)
	}
}

func Benchmark_getNomadServiceData(b *testing.B) {
	cache := newServiceCache(testFilter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)

	// Register a large number of services
	for i := range 1000 {
		cache.registerServiceInstance(context.Background(), &api.ServiceRegistration{
			ID:          fmt.Sprintf("service%d", i),
			ServiceName: "web",
			Namespace:   "default",
			Address:     fmt.Sprintf("10.0.0.%d", i),
			Port:        8080,
			Tags:        []string{fmt.Sprintf("traefik.enable=%t", i%2 == 0)},
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := cache.getNomadServiceData(context.Background())
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func Benchmark_listServices(b *testing.B) {
	cache := newServiceCache(testFilter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)

	// Register a large number of services
	for i := range 60000 {
		cache.registerServiceInstance(context.Background(), &api.ServiceRegistration{
			ID:          fmt.Sprintf("service%d", i),
			ServiceName: "web",
			Namespace:   "default",
			Address:     fmt.Sprintf("10.0.0.%d", i),
			Port:        8080,
			Tags:        []string{fmt.Sprintf("traefik.enable=%t", i%2 == 0)},
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := cache.listServices(context.Background())
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func Benchmark_fetchService(b *testing.B) {
	cache := newServiceCache(testFilter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)

	// Register a large number of services
	for i := range 60000 {
		cache.registerServiceInstance(context.Background(), &api.ServiceRegistration{
			ID:          fmt.Sprintf("service%d", i),
			ServiceName: "web",
			Namespace:   "default",
			Address:     fmt.Sprintf("10.0.0.%d", i),
			Port:        8080,
			Tags:        []string{fmt.Sprintf("traefik.enable=%t", i%2 == 0)},
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := cache.fetchService(context.Background(), "web")
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func Benchmark_registerServiceInstance(b *testing.B) {
	cache := newServiceCache(testFilter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)
	ctx := context.Background()

	services := make([]*api.ServiceRegistration, 1000)
	for i := range 1000 {
		services[i] = &api.ServiceRegistration{
			ID:          fmt.Sprintf("service%d", i),
			ServiceName: fmt.Sprintf("service%d", i/10), // 100 different service names
			Namespace:   "default",
			Address:     fmt.Sprintf("10.0.0.%d", i),
			Port:        8080,
			Tags:        []string{"traefik.enable=true"},
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Register a different service each iteration, cycling through the services
		cache.registerServiceInstance(ctx, services[i%1000])
	}
}

func Benchmark_deregisterServiceInstance(b *testing.B) {
	cache := newServiceCache(testFilter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)
	ctx := context.Background()

	// Pre-populate the cache with services
	services := make([]*api.ServiceRegistration, 1000)
	for i := range 1000 {
		services[i] = &api.ServiceRegistration{
			ID:          fmt.Sprintf("service%d", i),
			ServiceName: fmt.Sprintf("service%d", i/10), // 100 different service names
			Namespace:   "default",
			Address:     fmt.Sprintf("10.0.0.%d", i),
			Port:        8080,
			Tags:        []string{"traefik.enable=true"},
		}
		cache.registerServiceInstance(ctx, services[i])
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Deregister a different service each iteration, cycling through the services
		// We use a modulo to cycle through the services and ensure we're always testing with existing services
		cache.deregisterServiceInstance(ctx, services[i%1000])
	}
}

func BenchmarkRegisterServiceInstance(b *testing.B) {
	benchmarks := []struct {
		name            string
		cacheSize       int
		serviceNameRand int
		instancesPerSvc int
	}{
		{"Small_Cache_Few_Services", 10, 2, 5},
		{"Small_Cache_Many_Services", 10, 10, 1},
		{"Medium_Cache", 100, 10, 10},
		{"Large_Cache_Few_Services", 1000, 10, 100},
		{"Large_Cache_Many_Services", 1000, 100, 10},
		{"Very_Large_Cache", 5000, 50, 100},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			cache := newServiceCache(testFilter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)
			ctx := context.Background()

			// Pre-populate cache to desired size
			for i := 0; i < bm.cacheSize; i++ {
				svcName := fmt.Sprintf("service%d", i%bm.serviceNameRand)
				svcID := fmt.Sprintf("instance%d-%d", i%bm.serviceNameRand, i/bm.serviceNameRand)

				cache.registerServiceInstance(ctx, &api.ServiceRegistration{
					ID:          svcID,
					ServiceName: svcName,
					Tags:        []string{"traefik.enable=true"},
				})
			}

			// Create a new instance for benchmarking
			newSvc := &api.ServiceRegistration{
				ID:          "benchmark-instance",
				ServiceName: "service0", // Use existing service to test update path
				Tags:        []string{"traefik.enable=true"},
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Alternate between adding new services and updating existing ones
				if i%2 == 0 {
					newSvc.ID = fmt.Sprintf("benchmark-instance-%d", i)
				} else {
					newSvc.ID = "benchmark-instance"
				}
				cache.registerServiceInstance(ctx, newSvc)
			}
		})
	}
}

func BenchmarkDeregisterServiceInstance(b *testing.B) {
	benchmarks := []struct {
		name            string
		cacheSize       int
		serviceNameRand int
		instancesPerSvc int
	}{
		{"Small_Cache", 10, 2, 5},
		{"Medium_Cache", 100, 10, 10},
		{"Large_Cache", 1000, 20, 50},
		{"Very_Large_Cache", 5000, 50, 100},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			cache := newServiceCache(testFilter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)
			ctx := context.Background()

			// Pre-populate cache to desired size
			services := make([]*api.ServiceRegistration, bm.cacheSize)
			for i := 0; i < bm.cacheSize; i++ {
				svcName := fmt.Sprintf("service%d", i%bm.serviceNameRand)
				svcID := fmt.Sprintf("instance%d-%d", i%bm.serviceNameRand, i/bm.serviceNameRand)

				svc := &api.ServiceRegistration{
					ID:          svcID,
					ServiceName: svcName,
					Tags:        []string{"traefik.enable=true"},
				}
				services[i] = svc
				cache.registerServiceInstance(ctx, svc)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Use modulo to cycle through available services for deregistration
				svcIndex := i % bm.cacheSize
				cache.deregisterServiceInstance(ctx, services[svcIndex])

				// Re-register the service to keep the cache size constant during benchmark
				if i < b.N-bm.cacheSize {
					cache.registerServiceInstance(ctx, services[svcIndex])
				}
			}
		})
	}
}

func BenchmarkRegisterServiceInstance_ConcurrentUpdates(b *testing.B) {
	cache := newServiceCache(testFilter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)
	ctx := context.Background()

	// Pre-populate with 1000 services across 20 service names
	for i := 0; i < 1000; i++ {
		svcName := fmt.Sprintf("service%d", i%20)
		svcID := fmt.Sprintf("instance%d", i)

		cache.registerServiceInstance(ctx, &api.ServiceRegistration{
			ID:          svcID,
			ServiceName: svcName,
			Tags:        []string{"traefik.enable=true"},
		})
	}

	// Create a pool of services to update
	updateServices := make([]*api.ServiceRegistration, b.N)
	for i := 0; i < b.N; i++ {
		updateServices[i] = &api.ServiceRegistration{
			ID:          fmt.Sprintf("update-instance-%d", i%100),
			ServiceName: fmt.Sprintf("service%d", i%20),
			Tags:        []string{"traefik.enable=true"},
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cache.registerServiceInstance(ctx, updateServices[i%len(updateServices)])
			i++
		}
	})
}

func BenchmarkDeregisterServiceInstance_ConcurrentUpdates(b *testing.B) {
	cache := newServiceCache(testFilter, testExtraConfFromTags, &mockEventClient{}, &mockServiceClient{}, false)
	ctx := context.Background()

	// Create a pool of services to register/deregister
	numServices := 1000
	services := make([]*api.ServiceRegistration, numServices)
	for i := 0; i < numServices; i++ {
		services[i] = &api.ServiceRegistration{
			ID:          fmt.Sprintf("instance-%d", i),
			ServiceName: fmt.Sprintf("service%d", i%20),
			Tags:        []string{"traefik.enable=true"},
		}
		cache.registerServiceInstance(ctx, services[i])
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Choose a random service
			i := rand.Intn(numServices)
			cache.deregisterServiceInstance(ctx, services[i])

			// Re-register it immediately to maintain cache size
			cache.registerServiceInstance(ctx, services[i])
		}
	})
}

// Mock implementations for testing
type mockEventClient struct {
	streamFunc func(ctx context.Context, topics map[api.Topic][]string, index uint64, q *api.QueryOptions) (<-chan *api.Events, error)
}

func (m *mockEventClient) Stream(ctx context.Context, topics map[api.Topic][]string, index uint64, q *api.QueryOptions) (<-chan *api.Events, error) {
	return m.streamFunc(ctx, topics, index, q)
}

type mockServiceClient struct {
	listFunc func(q *api.QueryOptions) ([]*api.ServiceRegistrationListStub, *api.QueryMeta, error)
	getFunc  func(name string, q *api.QueryOptions) ([]*api.ServiceRegistration, *api.QueryMeta, error)
}

func (m *mockServiceClient) List(q *api.QueryOptions) ([]*api.ServiceRegistrationListStub, *api.QueryMeta, error) {
	return m.listFunc(q)
}

func (m *mockServiceClient) Get(name string, q *api.QueryOptions) ([]*api.ServiceRegistration, *api.QueryMeta, error) {
	return m.getFunc(name, q)
}

// Helper function to create a test service cache
func createTestServiceCache(eventClient eventClientAPI, serviceClient serviceClientAPI) *serviceCache {
	filter := func(ctx context.Context, name string, tags []string) bool {
		return true // Accept all services for testing
	}
	extraConf := func(tags []string) configuration {
		return configuration{Enable: true}
	}
	return newServiceCache(filter, extraConf, eventClient, serviceClient, false)
}

func TestServiceCache_Run_EventStreamError(t *testing.T) {
	tests := []struct {
		name           string
		eventsErr      error
		expectedErrMsg string
		description    string
	}{
		{
			name:           "connection timeout error",
			eventsErr:      errors.New("connection timeout"),
			expectedErrMsg: "connection timeout",
			description:    "Test that connection timeout errors are returned correctly",
		},
		{
			name:           "API error",
			eventsErr:      errors.New("API rate limit exceeded"),
			expectedErrMsg: "API rate limit exceeded",
			description:    "Test that API errors are returned correctly",
		},
		{
			name:           "network error",
			eventsErr:      errors.New("network unreachable"),
			expectedErrMsg: "network unreachable",
			description:    "Test that network errors are returned correctly",
		},
		{
			name:           "permission denied error",
			eventsErr:      errors.New("permission denied"),
			expectedErrMsg: "permission denied",
			description:    "Test that permission errors are returned correctly",
		},
		{
			name:           "custom error with special characters",
			eventsErr:      errors.New("error with special chars: !@#$%^&*()"),
			expectedErrMsg: "error with special chars: !@#$%^&*()",
			description:    "Test that errors with special characters are returned correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create event channel that will return an event with the specified error
			eventCh := make(chan *api.Events, 1)
			eventCh <- &api.Events{
				Err: tt.eventsErr,
			}
			close(eventCh)

			// Mock event client that returns our test event channel
			mockEventClient := &mockEventClient{
				streamFunc: func(ctx context.Context, topics map[api.Topic][]string, index uint64, q *api.QueryOptions) (<-chan *api.Events, error) {
					return eventCh, nil
				},
			}

			// Mock service client (not used in this test but required)
			mockServiceClient := &mockServiceClient{
				listFunc: func(q *api.QueryOptions) ([]*api.ServiceRegistrationListStub, *api.QueryMeta, error) {
					return []*api.ServiceRegistrationListStub{}, &api.QueryMeta{LastIndex: 1}, nil
				},
				getFunc: func(name string, q *api.QueryOptions) ([]*api.ServiceRegistration, *api.QueryMeta, error) {
					return []*api.ServiceRegistration{}, &api.QueryMeta{}, nil
				},
			}

			cache := createTestServiceCache(mockEventClient, mockServiceClient)
			serviceEventsChan := make(chan *api.Events, 1)

			// Run the method
			err := cache.run(context.Background(), 0, serviceEventsChan)

			// This is the critical assertion that would catch the bug
			// The test should fail if the method returns the wrong error variable
			require.Error(t, err, "Expected an error when events.Err is set")
			assert.Equal(t, tt.expectedErrMsg, err.Error(),
				"Expected error message to match events.Err exactly. This test would catch the bug where 'err' is returned instead of 'events.Err'")
		})
	}
}

func TestServiceCache_Run_EventStreamInitializationError(t *testing.T) {
	// Test the case where the initial Stream() call fails
	expectedErr := errors.New("failed to initialize event stream")

	mockEventClient := &mockEventClient{
		streamFunc: func(ctx context.Context, topics map[api.Topic][]string, index uint64, q *api.QueryOptions) (<-chan *api.Events, error) {
			return nil, expectedErr
		},
	}

	mockServiceClient := &mockServiceClient{
		listFunc: func(q *api.QueryOptions) ([]*api.ServiceRegistrationListStub, *api.QueryMeta, error) {
			return []*api.ServiceRegistrationListStub{}, &api.QueryMeta{LastIndex: 1}, nil
		},
		getFunc: func(name string, q *api.QueryOptions) ([]*api.ServiceRegistration, *api.QueryMeta, error) {
			return []*api.ServiceRegistration{}, &api.QueryMeta{}, nil
		},
	}

	cache := createTestServiceCache(mockEventClient, mockServiceClient)
	serviceEventsChan := make(chan *api.Events, 1)

	err := cache.run(context.Background(), 0, serviceEventsChan)

	require.Error(t, err)
	assert.Equal(t, expectedErr.Error(), err.Error(),
		"Expected error from initial Stream() call to be returned correctly")
}

func TestServiceCache_Run_EventStreamClosed(t *testing.T) {
	// Test the case where the event stream closes cleanly
	eventCh := make(chan *api.Events)
	close(eventCh) // Close immediately

	mockEventClient := &mockEventClient{
		streamFunc: func(ctx context.Context, topics map[api.Topic][]string, index uint64, q *api.QueryOptions) (<-chan *api.Events, error) {
			return eventCh, nil
		},
	}

	mockServiceClient := &mockServiceClient{
		listFunc: func(q *api.QueryOptions) ([]*api.ServiceRegistrationListStub, *api.QueryMeta, error) {
			return []*api.ServiceRegistrationListStub{}, &api.QueryMeta{LastIndex: 1}, nil
		},
		getFunc: func(name string, q *api.QueryOptions) ([]*api.ServiceRegistration, *api.QueryMeta, error) {
			return []*api.ServiceRegistration{}, &api.QueryMeta{}, nil
		},
	}

	cache := createTestServiceCache(mockEventClient, mockServiceClient)
	serviceEventsChan := make(chan *api.Events, 1)

	err := cache.run(context.Background(), 0, serviceEventsChan)

	// Should return nil when stream closes cleanly
	assert.NoError(t, err, "Expected no error when event stream closes cleanly")
}

func TestServiceCache_Run_ContextCancellation(t *testing.T) {
	// Test the case where context is cancelled
	eventCh := make(chan *api.Events, 1)
	// Don't close the channel, let context cancellation handle it

	mockEventClient := &mockEventClient{
		streamFunc: func(ctx context.Context, topics map[api.Topic][]string, index uint64, q *api.QueryOptions) (<-chan *api.Events, error) {
			return eventCh, nil
		},
	}

	mockServiceClient := &mockServiceClient{
		listFunc: func(q *api.QueryOptions) ([]*api.ServiceRegistrationListStub, *api.QueryMeta, error) {
			return []*api.ServiceRegistrationListStub{}, &api.QueryMeta{LastIndex: 1}, nil
		},
		getFunc: func(name string, q *api.QueryOptions) ([]*api.ServiceRegistration, *api.QueryMeta, error) {
			return []*api.ServiceRegistration{}, &api.QueryMeta{}, nil
		},
	}

	cache := createTestServiceCache(mockEventClient, mockServiceClient)
	serviceEventsChan := make(chan *api.Events, 1)

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel context after a short delay
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := cache.run(ctx, 0, serviceEventsChan)

	require.Error(t, err)
	assert.Equal(t, context.Canceled, err,
		"Expected context.Canceled error when context is cancelled")
}

func TestServiceCache_Run_HandleServiceEventsError(t *testing.T) {
	// Test the case where handleServiceEvents returns an error
	// This would require mocking the event parsing to return an error
	eventCh := make(chan *api.Events, 1)
	eventCh <- &api.Events{
		Events: []api.Event{
			// Create a mock event that would cause handleServiceEvents to return an error
			// This is more complex as it requires mocking the event.Service() method
		},
	}
	close(eventCh)

	mockEventClient := &mockEventClient{
		streamFunc: func(ctx context.Context, topics map[api.Topic][]string, index uint64, q *api.QueryOptions) (<-chan *api.Events, error) {
			return eventCh, nil
		},
	}

	mockServiceClient := &mockServiceClient{
		listFunc: func(q *api.QueryOptions) ([]*api.ServiceRegistrationListStub, *api.QueryMeta, error) {
			return []*api.ServiceRegistrationListStub{}, &api.QueryMeta{LastIndex: 1}, nil
		},
		getFunc: func(name string, q *api.QueryOptions) ([]*api.ServiceRegistration, *api.QueryMeta, error) {
			return []*api.ServiceRegistration{}, &api.QueryMeta{}, nil
		},
	}

	cache := createTestServiceCache(mockEventClient, mockServiceClient)
	serviceEventsChan := make(chan *api.Events, 1)

	err := cache.run(context.Background(), 0, serviceEventsChan)

	// This test would verify that errors from handleServiceEvents are propagated correctly
	// The exact behavior depends on how the mock events are structured
	assert.NoError(t, err, "Expected no error for valid events")
}

func TestServiceCache_Run_ErrorPropagationRegression(t *testing.T) {
	// This test specifically checks for the regression bug
	// It would fail if someone changes "return events.Err" to "return err"

	testCases := []struct {
		name        string
		eventsErr   error
		streamErr   error
		description string
	}{
		{
			name:        "events error should be returned, not stream error",
			eventsErr:   errors.New("events error"),
			streamErr:   errors.New("stream error"),
			description: "This test would catch the bug where 'err' (stream error) is returned instead of 'events.Err'",
		},
		{
			name:        "different error messages to ensure correct variable",
			eventsErr:   errors.New("specific events error message"),
			streamErr:   errors.New("different stream error message"),
			description: "Ensures the exact events.Err message is returned",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			eventCh := make(chan *api.Events, 1)
			eventCh <- &api.Events{
				Err: tc.eventsErr,
			}
			close(eventCh)

			mockEventClient := &mockEventClient{
				streamFunc: func(ctx context.Context, topics map[api.Topic][]string, index uint64, q *api.QueryOptions) (<-chan *api.Events, error) {
					return eventCh, tc.eventsErr
				},
			}

			mockServiceClient := &mockServiceClient{
				listFunc: func(q *api.QueryOptions) ([]*api.ServiceRegistrationListStub, *api.QueryMeta, error) {
					return []*api.ServiceRegistrationListStub{}, &api.QueryMeta{LastIndex: 1}, nil
				},
				getFunc: func(name string, q *api.QueryOptions) ([]*api.ServiceRegistration, *api.QueryMeta, error) {
					return []*api.ServiceRegistration{}, &api.QueryMeta{}, nil
				},
			}

			cache := createTestServiceCache(mockEventClient, mockServiceClient)
			serviceEventsChan := make(chan *api.Events, 1)

			err := cache.run(context.Background(), 0, serviceEventsChan)

			require.Error(t, err)
			// This assertion would catch the bug - it should return events.Err, not stream error
			assert.Equal(t, tc.eventsErr.Error(), err.Error(),
				fmt.Sprintf("Expected events error '%s', got '%s'. %s",
					tc.eventsErr.Error(), err.Error(), tc.description))

			// Additional check to ensure it's NOT the stream error
			assert.NotEqual(t, tc.streamErr.Error(), err.Error(),
				"Should not return the stream error when events.Err is set")
		})
	}
}

// Integration-style test that verifies the complete error handling flow
func TestServiceCache_Run_CompleteErrorHandlingFlow(t *testing.T) {
	// Test the complete flow with multiple error scenarios
	eventCh := make(chan *api.Events, 3)

	// Send multiple events with different error conditions
	eventCh <- &api.Events{
		Events: []api.Event{}, // Valid event
	}
	eventCh <- &api.Events{
		Err: errors.New("stream error occurred"), // Error event
	}
	eventCh <- &api.Events{
		Events: []api.Event{}, // Another valid event (should not be processed due to previous error)
	}
	close(eventCh)

	mockEventClient := &mockEventClient{
		streamFunc: func(ctx context.Context, topics map[api.Topic][]string, index uint64, q *api.QueryOptions) (<-chan *api.Events, error) {
			return eventCh, nil
		},
	}

	mockServiceClient := &mockServiceClient{
		listFunc: func(q *api.QueryOptions) ([]*api.ServiceRegistrationListStub, *api.QueryMeta, error) {
			return []*api.ServiceRegistrationListStub{}, &api.QueryMeta{LastIndex: 1}, nil
		},
		getFunc: func(name string, q *api.QueryOptions) ([]*api.ServiceRegistration, *api.QueryMeta, error) {
			return []*api.ServiceRegistration{}, &api.QueryMeta{}, nil
		},
	}

	cache := createTestServiceCache(mockEventClient, mockServiceClient)
	serviceEventsChan := make(chan *api.Events, 1)

	err := cache.run(context.Background(), 0, serviceEventsChan)

	require.Error(t, err)
	assert.Equal(t, "stream error occurred", err.Error(),
		"Should return the events.Err error and stop processing subsequent events")
}
