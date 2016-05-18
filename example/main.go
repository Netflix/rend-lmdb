// Copyright 2016 Netflix, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"github.com/netflix/rend-lmdb/lmdbh"
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/orcas"
	"github.com/netflix/rend/server"
)

func main() {
	largs := server.ListenArgs{
		Type: server.ListenTCP,
		Port: 12121,
	}

	server.ListenAndServe(
		largs,
		server.Default,
		orcas.L1Only,
		lmdbh.New("/tmp/rendb/", 2*1024*1024*1024),
		handlers.NilHandler,
	)
}
