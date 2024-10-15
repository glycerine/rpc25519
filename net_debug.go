// copied from standard library net/rpc so we can hack on it.

// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
/*
Copyright (c) 2009 The Go Authors. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

  - Redistributions of source code must retain the above copyright

notice, this list of conditions and the following disclaimer.
  - Redistributions in binary form must reproduce the above

copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
  - Neither the name of Google Inc. nor the names of its

contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package rpc25519

/*
	Some HTML presented at http://machine:port/debug/rpc
	Lists services, their methods, and some statistics, still rudimentary.
*/

import (
	"fmt"
	"html/template"
	"net/http"
	"slices"
	"strings"
)

const debugText = `<html>
	<body>
	<title>Services</title>
	{{range .}}
	<hr>
	Service {{.Name}}
	<hr>
		<table>
		<th align=center>Method</th><th align=center>Calls</th>
		{{range .Method}}
			<tr>
			<td align=left font=fixed>{{.Name}}({{.Type.ArgType}}, {{.Type.ReplyType}}) error</td>
			<td align=center>{{.Type.NumCalls}}</td>
			</tr>
		{{end}}
		</table>
	{{end}}
	</body>
	</html>`

var debug = template.Must(template.New("RPC debug").Parse(debugText))

// If set, print log statements for internal and I/O errors.
var debugLog = false

type debugMethod struct {
	Type *methodType
	Name string
}

type methodArray []debugMethod

type debugService struct {
	Service *service
	Name    string
	Method  []debugMethod
}

type serviceArray []debugService

func (s serviceArray) Len() int           { return len(s) }
func (s serviceArray) Less(i, j int) bool { return s[i].Name < s[j].Name }
func (s serviceArray) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (m methodArray) Len() int           { return len(m) }
func (m methodArray) Less(i, j int) bool { return m[i].Name < m[j].Name }
func (m methodArray) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }

type debugHTTP struct {
	*NetServer
}

// Runs at /debug/rpc
func (server debugHTTP) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Build a sorted version of the data.
	var services serviceArray
	server.serviceMap.Range(func(snamei, svci any) bool {
		svc := svci.(*service)
		ds := debugService{svc, snamei.(string), make([]debugMethod, 0, len(svc.method))}
		for mname, method := range svc.method {
			ds.Method = append(ds.Method, debugMethod{method, mname})
		}
		slices.SortFunc(ds.Method, func(a, b debugMethod) int {
			return strings.Compare(a.Name, b.Name)
		})
		services = append(services, ds)
		return true
	})
	slices.SortFunc(services, func(a, b debugService) int {
		return strings.Compare(a.Name, b.Name)
	})
	err := debug.Execute(w, services)
	if err != nil {
		fmt.Fprintln(w, "rpc: error executing template:", err.Error())
	}
}
