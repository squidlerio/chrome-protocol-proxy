package main

import (
	"context"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
)

const (
	incomingBufferSize = 10 * 1024 * 1024
	outgoingBufferSize = 25 * 1024 * 1024
)

var wsUpgrader = &websocket.Upgrader{
	ReadBufferSize:  incomingBufferSize,
	WriteBufferSize: outgoingBufferSize,
	CheckOrigin: func(r *http.Request) bool {
		// Chrome refuses a DevTools upgrade whose request carries an Origin, unless it was
		// started with --remote-allow-origins. That is what stops a page from driving CDP
		// over the browser it is running in.
		//
		// This proxy stands in front of Chrome and dials it without forwarding the header,
		// so Chrome never sees the Origin and never applies that rule. Accepting one here
		// therefore gives the protection away: wherever the proxy is reachable from a page's
		// own network namespace — a bridged container republishing the debugger, say — the
		// page could connect and take browser-level control of the session.
		//
		// A browser always sends Origin on a WebSocket handshake and cannot be told not to.
		// A CDP client is not a browser and sends none. So refusing the header is the same
		// rule Chrome applies, restored at the only point that still sees it.
		origin := r.Header.Get("Origin")
		if origin != "" {
			log.Printf("refusing websocket upgrade from %s: Origin %q present", r.RemoteAddr, origin)
			return false
		}
		return true
	},
}

var wsDialer = &websocket.Dialer{
	ReadBufferSize:  outgoingBufferSize,
	WriteBufferSize: incomingBufferSize,
}

func proxyWS(ctxt context.Context, stream chan *protocolMessage, in, out *websocket.Conn, errc chan error) {
	var mt int
	var buf []byte
	var err error

	for {
		select {
		default:
			mt, buf, err = in.ReadMessage()
			if err != nil {
				errc <- err
				return
			}

			if msg, derr := decodeMessage(buf); derr == nil {
				stream <- msg
			}

			err = out.WriteMessage(mt, buf)

			if err != nil {
				errc <- err
				return
			}

		case <-ctxt.Done():
			return
		}
	}
}
