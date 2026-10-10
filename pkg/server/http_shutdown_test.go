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
	"bufio"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	certutil "k8s.io/client-go/util/cert"
)

func TestRunServerForcedShutdownJoinsHandlers(t *testing.T) {
	for _, http2 := range []bool{false, true} {
		t.Run(map[bool]string{false: "HTTP1", true: "HTTP2"}[http2], func(t *testing.T) {
			certificate, key, err := certutil.GenerateSelfSignedCertKey("localhost", []net.IP{net.ParseIP("127.0.0.1")}, nil)
			require.NoError(t, err)
			cert, err := dynamiccertificates.NewStaticCertKeyContent("shutdown-test", certificate, key)
			require.NoError(t, err)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			stop := make(chan struct{})
			var stopOnce sync.Once
			stopServer := func() { stopOnce.Do(func() { close(stop) }) }
			defer stopServer()
			cleanupAllowed := make(chan struct{})
			var cleanupOnce sync.Once
			release := func() { cleanupOnce.Do(func() { close(cleanupAllowed) }) }
			defer release()
			connections := make(chan string, 2)
			canceled := make(chan struct{}, 2)
			exited := make(chan struct{}, 2)
			serving := &SecureServingInfo{Listener: listener, Cert: cert, DisableHTTP2: !http2}
			stopped, _, err := serving.Serve(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer func() { exited <- struct{}{} }()
				connections <- r.RemoteAddr
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				// A non-reading client eventually blocks this write. Force-close must
				// release it before the owner's completion signal can be truthful.
				payload := []byte(strings.Repeat("x", 32*1024))
				for {
					if _, err := w.Write(payload); err != nil {
						break
					}
				}
				<-r.Context().Done()
				canceled <- struct{}{}
				<-cleanupAllowed
			}), 40*time.Millisecond, stop)
			require.NoError(t, err)
			transport := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, ForceAttemptHTTP2: http2}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport}
			responses := []*http.Response{}
			defer func() {
				for _, response := range responses {
					response.Body.Close()
				}
			}()
			requestContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// HTTP2 runs two concurrent streams on one connection, each independently owned.
			requests := 1
			if http2 {
				requests = 2
			}
			for i := 0; i < requests; i++ {
				req, _ := http.NewRequestWithContext(requestContext, "GET", "https://"+listener.Addr().String(), nil)
				response, err := client.Do(req)
				require.NoError(t, err)
				responses = append(responses, response)
				if http2 {
					require.Equal(t, 2, response.ProtoMajor)
				} else {
					require.Equal(t, 1, response.ProtoMajor)
				}
			}
			firstConnection := <-connections
			if http2 {
				require.Equal(t, firstConnection, <-connections, "HTTP2 callbacks must share one connection")
			}
			stopServer()
			for i := 0; i < requests; i++ {
				select {
				case <-canceled:
				case <-time.After(2 * time.Second):
					t.Fatal("forced shutdown did not release the unread HTTP stream")
				}
			}
			select {
			case <-stopped:
				t.Fatal("shutdown completed before admitted handler cleanup finished")
			default:
			}
			release()
			select {
			case <-stopped:
			case <-time.After(2 * time.Second):
				t.Fatal("shutdown did not join released handlers")
			}
			require.Equal(t, requests, len(exited), "completion must follow every non-hijacked callback")
		})
	}
}

func TestRunServerGracefulShutdown(t *testing.T) {
	started, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		io.WriteString(w, "completed")
		close(exited)
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	httpServer := &http.Server{Handler: handler}
	stop := make(chan struct{})
	stopped, _, err := RunServer(httpServer, listener, time.Second, stop)
	require.NoError(t, err)
	defer httpServer.Close()
	requestDone := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + listener.Addr().String())
		if err == nil {
			defer resp.Body.Close()
			data, readErr := io.ReadAll(resp.Body)
			err = readErr
			if string(data) != "completed" {
				err = io.ErrUnexpectedEOF
			}
		}
		requestDone <- err
	}()
	<-started
	close(stop)
	close(release)
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("graceful stop timed out")
	}
	require.NoError(t, <-requestDone)
	select {
	case <-exited:
	default:
		t.Fatal("handler did not complete")
	}
}

func TestRunServerHijackedOwnershipAndCallbacks(t *testing.T) {
	type contextKey struct{}
	var connected, hijacked atomic.Int64
	handedOff := make(chan struct{})
	finished := make(chan struct{})
	continueOwner := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(continueOwner) })
	failures := make(chan error, 1)
	httpServer := &http.Server{
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			connected.Add(1)
			return context.WithValue(ctx, contextKey{}, "preserved")
		},
		ConnState: func(c net.Conn, s http.ConnState) {
			if s == http.StateHijacked {
				hijacked.Add(1)
			}
		},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(finished)
			if r.Context().Value(contextKey{}) != "preserved" {
				failures <- io.ErrUnexpectedEOF
				return
			}
			conn, buffer, err := w.(http.Hijacker).Hijack()
			if err != nil {
				failures <- err
				return
			}
			defer conn.Close()
			close(handedOff)
			<-continueOwner
			line, err := buffer.ReadString('\n')
			if err == nil && line == "ping\n" {
				_, err = buffer.WriteString("pong\n")
				if err == nil {
					err = buffer.Flush()
				}
			}
			failures <- err
		}),
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	stop := make(chan struct{})
	stopped, _, err := RunServer(httpServer, listener, time.Second, stop)
	require.NoError(t, err)
	defer httpServer.Close()
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	require.NoError(t, err)
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(4 * time.Second))
	_, err = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
	require.NoError(t, err)
	select {
	case <-handedOff:
	case <-time.After(2 * time.Second):
		t.Fatal("request did not hand off its socket")
	}
	close(stop)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown waited for caller-owned hijacked connection")
	}
	require.Equal(t, int64(1), connected.Load())
	require.Equal(t, int64(1), hijacked.Load())
	releaseOnce.Do(func() { close(continueOwner) })
	_, err = io.WriteString(conn, "ping\n")
	require.NoError(t, err)
	line, err := bufio.NewReader(conn).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "pong\n", line)
	require.NoError(t, <-failures)
	<-finished
}

func TestRunServerLateAdmissionAndDefaultHandler(t *testing.T) {
	originalMux := http.DefaultServeMux
	http.DefaultServeMux = http.NewServeMux()
	defer func() { http.DefaultServeMux = originalMux }()
	var calls atomic.Int64
	http.DefaultServeMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { calls.Add(1); io.WriteString(w, "default") })
	httpServer := &http.Server{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	stop := make(chan struct{})
	stopped, _, err := RunServer(httpServer, listener, time.Second, stop)
	require.NoError(t, err)
	defer httpServer.Close()
	response, err := http.Get("http://" + listener.Addr().String())
	require.NoError(t, err)
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	require.NoError(t, err)
	require.Equal(t, "default", string(body))
	close(stop)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("server shutdown timed out")
	}
	late := httptest.NewRecorder()
	httpServer.Handler.ServeHTTP(late, httptest.NewRequest("GET", "/", nil))
	require.Equal(t, http.StatusServiceUnavailable, late.Code)
	require.Equal(t, int64(1), calls.Load(), "late admission must not invoke owned handlers")
}
