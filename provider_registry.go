package nntppool

import (
	"context"
	"fmt"
	"maps"
	"time"
)

// resolvedProvider is the immutable identity assigned to one provider
// registration. Reconnects reuse it instead of resolving Provider again;
// providerRegistry.orderedIDs owns stable order.
type resolvedProvider struct {
	provider Provider
	name     string
	id       string
}

type providerRegistration struct {
	spec  resolvedProvider
	group *providerGroup
	epoch uint64
}

// providerRegistry is an immutable snapshot. Production readers load this
// single authority, so main/backup ordering and token lookup cannot observe
// different generations.
type providerRegistry struct {
	mains        []*providerGroup
	backups      []*providerGroup
	orderedIDs   []string
	byID         map[string]providerRegistration
	ownerByToken map[string]string
}

func newProviderRegistry() *providerRegistry {
	return &providerRegistry{
		byID:         make(map[string]providerRegistration),
		ownerByToken: make(map[string]string),
	}
}

func (r *providerRegistry) clone() *providerRegistry {
	return &providerRegistry{
		orderedIDs:   append([]string(nil), r.orderedIDs...),
		byID:         maps.Clone(r.byID),
		ownerByToken: maps.Clone(r.ownerByToken),
	}
}

func (r *providerRegistry) rebuildGroups() {
	r.mains = make([]*providerGroup, 0, len(r.orderedIDs))
	r.backups = make([]*providerGroup, 0, len(r.orderedIDs))
	for _, id := range r.orderedIDs {
		registration := r.byID[id]
		if registration.group == nil {
			continue
		}
		if registration.spec.provider.Backup {
			r.backups = append(r.backups, registration.group)
		} else {
			r.mains = append(r.mains, registration.group)
		}
	}
}

func (c *Client) publishRegistryLocked(next *providerRegistry) {
	next.rebuildGroups()
	c.registry.Store(next)

	// These pointers remain only as package-test compatibility mirrors.
	// Production routing and lookup always read c.registry.
	c.mainGroups.Store(&next.mains)
	c.backupGroups.Store(&next.backups)
}

// resolveInitialProviders reserves every fixed ID and operational name before
// assigning generated names. It performs no network work.
func resolveInitialProviders(providers []Provider) ([]resolvedProvider, map[string]string, error) {
	resolved := make([]resolvedProvider, len(providers))
	tokenOwner := make(map[string]int, len(providers)*2)
	reserve := func(token string, providerIndex int) error {
		if token == "" {
			return nil
		}
		if previous, exists := tokenOwner[token]; exists && previous != providerIndex {
			return fmt.Errorf(
				"nntp: provider identity %q is shared by providers %d and %d",
				token,
				previous,
				providerIndex,
			)
		}
		tokenOwner[token] = providerIndex
		return nil
	}

	for i, provider := range providers {
		if err := validateProvider(provider); err != nil {
			return nil, nil, err
		}

		name := ""
		if provider.Host != "" {
			name = resolveProviderName(provider, i)
		}
		id := provider.ID
		if id == "" {
			id = name
		}
		if err := reserve(id, i); err != nil {
			return nil, nil, err
		}
		if err := reserve(name, i); err != nil {
			return nil, nil, err
		}
		resolved[i] = resolvedProvider{
			provider: provider,
			name:     name,
			id:       id,
		}
	}

	for i := range resolved {
		if resolved[i].name != "" {
			continue
		}
		id := resolved[i].id
		for candidate := i; ; candidate++ {
			name := fmt.Sprintf("provider-%d", candidate)
			if owner, exists := tokenOwner[name]; exists && owner != i {
				continue
			}
			if id == "" {
				id = name
			}
			if err := reserve(id, i); err != nil {
				return nil, nil, err
			}
			if err := reserve(name, i); err != nil {
				return nil, nil, err
			}
			resolved[i].name = name
			resolved[i].id = id
			break
		}
	}

	owners := make(map[string]string, len(tokenOwner))
	for token, owner := range tokenOwner {
		owners[token] = resolved[owner].id
	}
	return resolved, owners, nil
}

func validateProvider(provider Provider) error {
	if provider.Connections <= 0 {
		return fmt.Errorf("nntp: provider connections must be > 0")
	}
	if provider.Factory == nil && provider.Host == "" {
		return fmt.Errorf("nntp: provider must have Host or Factory")
	}
	return nil
}

func (c *Client) resolveAddedProviderLocked(provider Provider) (resolvedProvider, error) {
	if err := validateProvider(provider); err != nil {
		return resolvedProvider{}, err
	}

	current := c.registry.Load()
	name := ""
	if provider.Host != "" {
		name = resolveProviderName(provider, 0)
	}
	id := provider.ID
	if id == "" {
		id = name
	}
	if id != "" {
		if _, exists := current.byID[id]; exists {
			return resolvedProvider{}, fmt.Errorf("nntp: provider identity %q already exists", id)
		}
	}

	if name == "" {
		for {
			candidate := fmt.Sprintf("provider-%d", c.nextGenerated)
			c.nextGenerated++
			if _, exists := current.ownerByToken[candidate]; exists {
				continue
			}
			name = candidate
			if id == "" {
				id = name
			}
			break
		}
	}

	for _, token := range [...]string{id, name} {
		if _, exists := current.ownerByToken[token]; exists {
			return resolvedProvider{}, fmt.Errorf("nntp: provider identity %q already exists", token)
		}
	}

	spec := resolvedProvider{
		provider: provider,
		name:     name,
		id:       id,
	}
	return spec, nil
}

func (c *Client) reserveProviderLocked(spec resolvedProvider) {
	next := c.registry.Load().clone()
	next.ownerByToken[spec.id] = spec.id
	next.ownerByToken[spec.name] = spec.id
	next.byID[spec.id] = providerRegistration{
		spec:  spec,
		epoch: 1,
	}
	next.orderedIDs = append(next.orderedIDs, spec.id)
	c.publishRegistryLocked(next)
}

func (c *Client) completeProviderStart(id string, epoch uint64, ping PingResult) bool {
	c.registryMu.Lock()
	current := c.registry.Load()
	registration, exists := current.byID[id]
	if c.closed || c.ctx.Err() != nil || !exists || registration.group != nil || registration.epoch != epoch {
		c.registryMu.Unlock()
		return false
	}

	group := c.startProviderGroup(registration.spec, ping)
	if c.ctx.Err() != nil {
		c.registryMu.Unlock()
		group.cancel()
		group.gate.stop()
		return false
	}

	next := current.clone()
	registration.group = group
	next.byID[id] = registration
	c.publishRegistryLocked(next)
	c.registryMu.Unlock()
	return true
}

func removeRegistration(registry *providerRegistry, id string) {
	registration, exists := registry.byID[id]
	if !exists {
		return
	}
	delete(registry.byID, id)
	for _, token := range [...]string{registration.spec.id, registration.spec.name} {
		if token == "" {
			continue
		}
		if registry.ownerByToken[token] == id {
			delete(registry.ownerByToken, token)
		}
	}
	for i, orderedID := range registry.orderedIDs {
		if orderedID == id {
			registry.orderedIDs = append(registry.orderedIDs[:i], registry.orderedIDs[i+1:]...)
			break
		}
	}
}

// deactivateProvider removes an unavailable group from routing. When a
// reconnect is pending, its registration and token reservations remain.
func (c *Client) deactivateProvider(group *providerGroup, retain bool) (uint64, bool) {
	c.registryMu.Lock()
	current := c.registry.Load()
	registration, exists := current.byID[group.id]
	if c.closed || !exists || registration.group != group {
		c.registryMu.Unlock()
		return 0, false
	}

	next := current.clone()
	if retain {
		registration.group = nil
		registration.epoch++
		next.byID[group.id] = registration
	} else {
		removeRegistration(next, group.id)
	}
	c.publishRegistryLocked(next)
	c.registryMu.Unlock()

	group.cancel()
	group.gate.stop()
	return registration.epoch, true
}

func (c *Client) retireUnavailableProvider(group *providerGroup) {
	retain := group.p.ReconnectDelay > 0
	epoch, changed := c.deactivateProvider(group, retain)
	if changed && retain {
		c.scheduleReconnect(group.id, epoch, group.p.ReconnectDelay)
	}
}

func (c *Client) scheduleReconnect(id string, epoch uint64, delay time.Duration) {
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-c.ctx.Done():
			return
		case <-timer.C:
			c.reconnectProvider(id, epoch)
		}
	}()
}

func (c *Client) reconnectProvider(id string, epoch uint64) {
	c.registryMu.Lock()
	current := c.registry.Load()
	registration, exists := current.byID[id]
	if c.closed || c.ctx.Err() != nil || !exists || registration.group != nil || registration.epoch != epoch {
		c.registryMu.Unlock()
		return
	}

	next := current.clone()
	registration.epoch++
	next.byID[id] = registration
	c.publishRegistryLocked(next)
	c.registryMu.Unlock()

	ping := c.pingResolvedProvider(c.ctx, registration.spec)
	_ = c.completeProviderStart(id, registration.epoch, ping)
}

func (c *Client) registrationForToken(token string) (providerRegistration, bool) {
	registry := c.registry.Load()
	id, exists := registry.ownerByToken[token]
	if !exists {
		return providerRegistration{}, false
	}
	registration, exists := registry.byID[id]
	return registration, exists
}

func (c *Client) removeProvider(token string) (*providerGroup, bool) {
	c.registryMu.Lock()
	if c.closed {
		c.registryMu.Unlock()
		return nil, false
	}
	current := c.registry.Load()
	id, exists := current.ownerByToken[token]
	if !exists {
		c.registryMu.Unlock()
		return nil, false
	}
	registration := current.byID[id]
	next := current.clone()
	removeRegistration(next, id)
	c.publishRegistryLocked(next)
	c.registryMu.Unlock()
	return registration.group, true
}

func (c *Client) closeRegistry() *providerRegistry {
	c.registryMu.Lock()
	defer c.registryMu.Unlock()
	c.closed = true
	return c.registry.Load()
}

func (c *Client) addProvider(provider Provider) error {
	c.registryMu.Lock()
	if c.closed || c.ctx.Err() != nil {
		c.registryMu.Unlock()
		return context.Canceled
	}

	spec, err := c.resolveAddedProviderLocked(provider)
	if err != nil {
		c.registryMu.Unlock()
		return err
	}
	c.reserveProviderLocked(spec)
	c.registryMu.Unlock()

	ping := c.pingResolvedProvider(c.ctx, spec)
	if !c.completeProviderStart(spec.id, 1, ping) {
		if err := c.ctx.Err(); err != nil {
			return err
		}
		return fmt.Errorf("nntp: provider %q was removed before startup completed", spec.id)
	}
	return nil
}
