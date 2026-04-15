package server

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

func (s *GrpcServer) ServeStatus(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/state", func(w http.ResponseWriter, r *http.Request) {
		term, isLeader := s.GetState()
		leader := s.Raft().Leader()
		fmt.Fprintf(w, "%d %v %s", term, isLeader, leader)
	})

	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		cmd, _ := io.ReadAll(r.Body)
		index, _, ok := s.Raft().Start(cmd)
		if !ok {
			http.Error(w, "not leader", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, "%d", index)
	})

	mux.HandleFunc("/logs/", func(w http.ResponseWriter, r *http.Request) {
		index, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/logs/"))
		val, ok := s.Logs(index)
		if !ok {
			http.Error(w, "not committed", http.StatusNotFound)
			return
		}
		w.Write(val.([]byte))
	})

	http.ListenAndServe(addr, mux)
}
