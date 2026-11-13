package bridge

import "context"

// resolverCaps caches the optional capabilities of the configured resolver,
// discovered once in NewNative instead of via scattered type assertions at
// every call site. A nil field means the resolver lacks the capability.
type resolverCaps struct {
	prepare    func(context.Context) context.Context // Prepare: decorate request contexts
	fallback   FallbackResolver                      // Fallback: answer a cloud refusal locally
	routed     RoutedResolver                        // Decide/ResolveDecision routing
	streamer   fallbackStreamer                      // FallbackStream: SSE streaming of fallbacks
	isRefusal  func(string) bool                     // IsRefusal: provider-specific refusal detector
	emptyInput func(context.Context)                 // EmptyInput: notify on empty final ASR
}

func probeResolver(r Resolver) resolverCaps {
	var c resolverCaps
	if r == nil {
		return c
	}
	if p, ok := r.(interface{ Prepare(context.Context) context.Context }); ok {
		c.prepare = p.Prepare
	}
	if f, ok := r.(FallbackResolver); ok {
		c.fallback = f
	}
	if rr, ok := r.(RoutedResolver); ok {
		c.routed = rr
	}
	if s, ok := r.(fallbackStreamer); ok {
		c.streamer = s
	}
	if d, ok := r.(interface{ IsRefusal(string) bool }); ok {
		c.isRefusal = d.IsRefusal
	}
	if e, ok := r.(interface{ EmptyInput(context.Context) }); ok {
		c.emptyInput = e.EmptyInput
	}
	return c
}
