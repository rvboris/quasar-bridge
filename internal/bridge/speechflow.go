package bridge

// speechFlow is the lifecycle automaton of the joined audio stream behind a
// streaming (deferred) answer. It owns exactly the flags that used to live
// scattered on nativeRequest — started / waiting / finalized / progress —
// and answers every lifecycle question with an action description; the
// caller (Native) performs the locks and the I/O.
//
// States, informally:
//
//	idle ──first claimed source──▶ speaking ◀──┐
//	                                 │        │ (next source claimed;
//	 source ends, gap expected      │        │  waiting disarmed)
//	└────────────────────────────▶ gap ───────┘
//	speaking/gap ──final source ended──▶ finalized
//
// Invariants (previously only comments, now enforced here):
//   - silence keepalive (gap) is armed at most once at a time;
//   - nothing is armed or emitted after finalized (silence after EOS
//     garbles the tail);
//   - the progress prime's end additionally releases progressAudioDone
//     exactly once.
type speechFlow struct {
	started   bool // first raw source claimed; audioStream is bound
	waiting   bool // silence keepalive armed (gap between sources)
	finalized bool // EOS already sent; terminal state
	progress  bool // current source is the discarded progress prime
}

// flowAction describes what the caller must do after a lifecycle event.
// All fields are independent; zero value means "nothing".
type flowAction struct {
	// ArmSilence starts the silence keepalive pump (bridges the gap until
	// the next source feeds frames).
	ArmSilence bool
	// CloseProgressDone closes progressAudioDone, unblocking finishDeferred.
	CloseProgressDone bool
	// Finalize means: this source was the final one — flush the joined Ogg
	// with EOS and close the stream toward the firmware.
	Finalize bool
}

// sourceStart records that a new raw source claimed the stream. A new
// source always disarms the silence keepalive.
func (f *speechFlow) sourceStart() {
	f.started = true
	f.waiting = false
}

// sourceEnded handles a source that ran to its own EOS (detected on feed).
// final reports whether this source was the request's final synthesis
// (finalSynth). Called before the caller inspects Finalize.
func (f *speechFlow) sourceEnded(final bool) flowAction {
	if f.finalized {
		return flowAction{}
	}
	if f.progress {
		return flowAction{ArmSilence: !f.waiting, CloseProgressDone: !f.waiting}
	}
	if !final {
		return flowAction{ArmSilence: f.started && !f.waiting}
	}
	f.finalized = true
	f.waiting = false // stop the silence keepalive before EOS
	return flowAction{Finalize: true}
}

// streamClosed handles an upstream StreamControl Close for the joined
// stream. reason is the close reason; final is finalSynth. Returns the
// action; FlushPending asks the caller to flush the Ogg joiner's pending
// page (progress prime tail) before anything else.
func (f *speechFlow) streamClosed(zeroReason, final bool) (flowAction, bool /*flushPending*/) {
	if f.finalized {
		return flowAction{}, false
	}
	if f.progress && zeroReason {
		return flowAction{ArmSilence: !f.waiting, CloseProgressDone: !f.waiting}, true
	}
	if !final {
		return flowAction{ArmSilence: f.started && !f.waiting}, false
	}
	f.finalized = true
	f.waiting = false
	return flowAction{Finalize: true}, false
}

// finalizeNoTail reports whether the answer ended with no speech tail left
// to synthesize and the joined audio can be flushed right now: the first
// source already ended, nothing is in flight, and EOS was not sent yet.
func (f *speechFlow) finalizeNoTail(sourceDone, inFlight bool) bool {
	if !f.started || f.finalized || inFlight || !sourceDone {
		return false
	}
	f.finalized = true
	f.waiting = false
	return true
}
