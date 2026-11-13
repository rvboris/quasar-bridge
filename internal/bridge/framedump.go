package bridge

// Temporary diagnostic: dump every station-bound frame to a file when
// QB_DUMP_DIR is set. Format matches fwclient -dump (4-byte BE length +
// payload), one file per connection. Lives in /tmp on the station; remove
// once the tail-overlap hunt is over.

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func osGetenvDumpDir() string { return os.Getenv("QB_DUMP_DIR") }

type frameDump struct {
	mu   sync.Mutex
	file *os.File
}

func newFrameDump(dir, direction string) *frameDump {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return &frameDump{}
	}
	name := filepath.Join(dir, fmt.Sprintf("%s-%d.bin", direction, time.Now().UnixMilli()))
	f, err := os.Create(name)
	if err != nil {
		return &frameDump{}
	}
	return &frameDump{file: f}
}

func (d *frameDump) write(data []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file == nil {
		return
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	d.file.Write(hdr[:])
	d.file.Write(data)
}

func (d *frameDump) close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file != nil {
		d.file.Close()
		d.file = nil
	}
}
