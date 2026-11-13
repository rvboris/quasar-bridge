package bridge

import "testing"

func TestSpeechFlowSourceEndedTransitions(t *testing.T) {
	t.Run("progress prime arms silence and releases progress once", func(t *testing.T) {
		var f speechFlow
		f.sourceStart()
		f.progress = true
		act := f.sourceEnded(false)
		if !act.ArmSilence || !act.CloseProgressDone || act.Finalize {
			t.Fatalf("act = %+v", act)
		}
		f.waiting = true
		act = f.sourceEnded(false)
		if act.ArmSilence || act.CloseProgressDone {
			t.Fatalf("second end must be a no-op: %+v", act)
		}
	})
	t.Run("intermediate chunk re-arms silence once", func(t *testing.T) {
		var f speechFlow
		f.sourceStart()
		act := f.sourceEnded(false)
		if !act.ArmSilence || act.Finalize {
			t.Fatalf("act = %+v", act)
		}
		f.waiting = true
		if act = f.sourceEnded(false); act.ArmSilence || act.Finalize {
			t.Fatalf("re-arm while waiting: %+v", act)
		}
	})
	t.Run("final source finalizes and terminal ignores everything", func(t *testing.T) {
		var f speechFlow
		f.sourceStart()
		act := f.sourceEnded(true)
		if !act.Finalize || act.ArmSilence {
			t.Fatalf("act = %+v", act)
		}
		if f.waiting {
			t.Fatal("waiting must disarm before EOS")
		}
		if act = f.sourceEnded(false); act != (flowAction{}) {
			t.Fatalf("post-final activity: %+v", act)
		}
	})
}

func TestSpeechFlowStreamClosedTransitions(t *testing.T) {
	t.Run("progress close reason 0 flushes and swallows", func(t *testing.T) {
		var f speechFlow
		f.sourceStart()
		f.progress = true
		act, flush := f.streamClosed(true, false)
		if !flush || !act.ArmSilence || !act.CloseProgressDone || act.Finalize {
			t.Fatalf("act = %+v flush = %v", act, flush)
		}
	})
	t.Run("intermediate close swallowed only while streaming", func(t *testing.T) {
		var f speechFlow
		f.sourceStart()
		act, flush := f.streamClosed(false, false)
		if flush || act.Finalize || !act.ArmSilence {
			t.Fatalf("act = %+v flush = %v", act, flush)
		}
	})
	t.Run("marked-TTS close finalizes even without finalSynth", func(t *testing.T) {
		var f speechFlow
		f.sourceStart()
		act, _ := f.streamClosed(false, true) // final = finalSynth || !streaming
		if !act.Finalize {
			t.Fatalf("act = %+v", act)
		}
	})
	t.Run("close after finalize is a no-op", func(t *testing.T) {
		var f speechFlow
		f.sourceStart()
		f.sourceEnded(true)
		act, flush := f.streamClosed(false, true)
		if act != (flowAction{}) || flush {
			t.Fatalf("post-final close: %+v", act)
		}
	})
}

func TestSpeechFlowFinalizeNoTail(t *testing.T) {
	var f speechFlow
	if f.finalizeNoTail(true, false) {
		t.Fatal("must not finalize before the first source")
	}
	f.sourceStart()
	if f.finalizeNoTail(false, false) {
		t.Fatal("must not finalize while a source is mid-stream")
	}
	if f.finalizeNoTail(true, true) {
		t.Fatal("must not finalize while a synth claim is in flight")
	}
	if !f.finalizeNoTail(true, false) {
		t.Fatal("expected finalize")
	}
	if f.waiting || !f.finalized {
		t.Fatal("finalize must disarm waiting and set finalized")
	}
	if f.finalizeNoTail(true, false) {
		t.Fatal("second finalize must be a no-op")
	}
}

func TestSpeechFlowSourceStartDisarmsWaiting(t *testing.T) {
	var f speechFlow
	f.sourceStart()
	f.waiting = true
	f.sourceStart()
	if f.waiting {
		t.Fatal("new source must disarm the silence keepalive")
	}
}
