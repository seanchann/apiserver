/********************************************************************
* Copyright (c) All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*         http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*******************************************************************/

package server

import (
	"context"
	"net"
	"net/http"
	"sync"

	utilwaitgroup "k8s.io/apimachinery/pkg/util/waitgroup"
)

type httpConnectionContextKey struct{}
type httpServerRequest struct{ admitted bool }
type httpServerConnection struct {
	requests map[*httpServerRequest]struct{}
	hijacked bool
}

// trackServerHandlers joins callback cleanup after network shutdown. ConnState
// alone is insufficient: HTTP/2 can close a connection before its stream handlers
// return. Writers are untouched, including all optional HTTP interfaces.
func trackServerHandlers(server *http.Server) func() {
	var mu sync.Mutex
	var requests utilwaitgroup.SafeWaitGroup
	connections := make(map[net.Conn]*httpServerConnection)
	previousContext, previousState := server.ConnContext, server.ConnState
	server.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
		if previousContext != nil {
			ctx = previousContext(ctx, conn)
		}
		state := &httpServerConnection{requests: make(map[*httpServerRequest]struct{})}
		mu.Lock()
		connections[conn] = state
		mu.Unlock()
		return context.WithValue(ctx, httpConnectionContextKey{}, state)
	}
	server.ConnState = func(conn net.Conn, state http.ConnState) {
		mu.Lock()
		connection := connections[conn]
		if connection != nil && state == http.StateHijacked {
			// Hijack transfers both the socket and the remainder of its handler to
			// the caller. A later handler return must not release these leases twice.
			connection.hijacked = true
			for request := range connection.requests {
				request.admitted = false
				delete(connection.requests, request)
				requests.Done()
			}
		}
		if state == http.StateClosed || state == http.StateHijacked {
			delete(connections, conn)
		}
		mu.Unlock()
		if previousState != nil {
			previousState(conn, state)
		}
	}
	handler := server.Handler
	if handler == nil {
		handler = http.DefaultServeMux
	}
	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// SafeWaitGroup rejects positive Add after Wait starts, including handlers
		// dispatched from already-read requests racing a forced connection close.
		if err := requests.Add(1); err != nil {
			http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
			return
		}
		connection := r.Context().Value(httpConnectionContextKey{}).(*httpServerConnection)
		request := &httpServerRequest{}
		mu.Lock()
		if connection.hijacked {
			requests.Done()
		} else {
			request.admitted = true
			connection.requests[request] = struct{}{}
		}
		mu.Unlock()
		defer func() {
			mu.Lock()
			if request.admitted {
				request.admitted = false
				delete(connection.requests, request)
				requests.Done()
			}
			mu.Unlock()
		}()
		handler.ServeHTTP(w, r)
	})
	return requests.Wait
}
