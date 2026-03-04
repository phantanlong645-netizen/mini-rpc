package Day5_http

import (
	"fmt"
	"html/template"
	"net/http"
)

const debugText = `<html>
    <body>
    <title>GeeRPC Services</title>
    {{range .}}
    <hr>
    Service {{.Name}}
    <hr>
       <table>
       <th align=center>Method</th><th align=center>Calls</th>
       {{range $name, $mtype := .Method}}
          <tr>
          <td align=left font=fixed>{{$name}}({{$mtype.Args}}, {{$mtype.Reply}}) error</td>
          <td align=center>{{$mtype.NumCalls}}</td>
          </tr>
       {{end}}
       </table>
    {{end}}
    </body>
    </html>`

var debug = template.Must(template.New("RPC debug").Parse(debugText))

type debugHttp struct {
	*Server
}
type debugService struct {
	Name   string
	Method map[string]*MethodType
}

func (server debugHttp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var services []debugService
	server.serviceMap.Range(func(namei, svci interface{}) bool {
		svc := svci.(*Service)
		services = append(services, debugService{
			Name:   namei.(string),
			Method: svc.methods,
		})
		return true
	})
	err := debug.Execute(w, services)
	if err != nil {
		_, _ = fmt.Fprintln(w, "rpc: error executing template:", err.Error())
	}
}
