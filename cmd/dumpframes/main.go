package main

import (
	"encoding/binary"
	"fmt"
	"os"

	"quasarbridge/internal/wire"
)

func main() {
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	off := 0
	for off+4 <= len(data) {
		n := int(binary.BigEndian.Uint32(data[off : off+4]))
		off += 4
		f := data[off : off+n]
		off += n
		if len(f) < 4 || string(f[:4]) == "AAPI" {
			fr := wire.Decode(f, wire.ServerToClient)
			e := fr.Server.GetEvent()
			if e == nil {
				continue
			}
			h := e.GetHeader()
			ref := h.GetSubRequestRef()
			fmt.Printf("EVENT %-22s msg=%s ref=%s subref.msg=%s subref.req=%s stream=%d\n", fr.Kind(), h.GetMessageId(), h.GetRefMessageId(), ref.GetMessageId(), ref.GetRequestId(), h.GetStreamId())
			continue
		}
		sid := binary.BigEndian.Uint32(f[:4])
		fmt.Printf("AUDIO stream=%d bytes=%d ogg=%v\n", sid, len(f)-4, len(f) >= 8 && string(f[4:8]) == "OggS")
	}
}
