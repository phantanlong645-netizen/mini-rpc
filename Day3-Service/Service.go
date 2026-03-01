package Jeerpc

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

type MethodType struct {
	Method   reflect.Method
	Args     reflect.Type
	Reply    reflect.Type
	NumCalls uint64
}

type Service struct {
	name    string
	rcvr    reflect.Value
	typ     reflect.Type
	methods map[string]*MethodType
}

func (m *MethodType) NewArgv() reflect.Value {
	var arg reflect.Value
	if m.Args.Kind() == reflect.Ptr {
		arg = reflect.New(m.Args.Elem())
	} else {
		arg = reflect.New(m.Args).Elem()
	}
	return arg
}

func (m *MethodType) NewReplyv() reflect.Value {
	var replyv = reflect.New(m.Reply.Elem())
	switch m.Reply.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.Reply.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.Reply.Elem(), 0, 0))
	}
	return replyv
}

func NewService(rcvr interface{}) *Service {
	s := new(Service)
	s.rcvr = reflect.ValueOf(rcvr)
	s.typ = reflect.TypeOf(rcvr)
	s.name = reflect.Indirect(s.rcvr).Type().Name()
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid service name", s.name)
	}
	s.RegisterMethod()
	return s
}

func (s *Service) RegisterMethod() {
	s.methods = make(map[string]*MethodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mtype := method.Type
		if mtype.NumIn() != 3 || mtype.NumOut() != 1 {
			continue
		}
		argtype := mtype.In(1)
		replytype := mtype.In(2)
		if mtype.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		if !isExportedOrBuiltinType(argtype) || !isExportedOrBuiltinType(replytype) {
			continue
		}
		s.methods[method.Name] = &MethodType{
			Method: method,
			Args:   argtype,
			Reply:  replytype,
		}
		log.Printf("rpc server: register %s.%s\n", s.name, method.Name)
	}
}
func (s *Service) Call(m *MethodType, argv, reply reflect.Value) error {
	atomic.AddUint64(&m.NumCalls, 1)
	f := m.Method.Func
	returnValues := f.Call([]reflect.Value{s.rcvr, argv, reply})
	if returnValues[0].Interface() != nil {
		return returnValues[0].Interface().(error)
	}
	return nil

}

func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}
