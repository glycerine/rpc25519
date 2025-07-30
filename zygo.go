package rpc25519

import (
	"fmt"

	"github.com/glycerine/zygomys/v9/zygo"
)

func init() {
	gsr := &zygo.GoStructRegistry
	pf := &zygo.RegisteredType{GenDefMap: true, Factory: func(env *zygo.Zlisp, h *zygo.SexpHash) (interface{}, error) {
		return NewConfig(), nil
	}}
	gsr.RegisterUserdef(pf, true, "rpc25519_Config")
}

func (c *Config) SexpString(ps *zygo.PrintState) (r string) {
	return fmt.Sprintf("(rpc25519_Config\n%v)", c.StringBody())
}

func (c *Config) ShortSexpString(ps *zygo.PrintState) (r string) {
	return fmt.Sprintf("(rpc25519_Config\n%v)", c.ShortStringBody())
}

// need only be called once before load/SexpString is used.
func RegisterConfigZygoConstructor(env *zygo.Zlisp) {
	if env != nil {
		env.AddFunction("rpc25519_Config", rpcConfigZygoFunction)
	}
	return
}

func rpcConfigZygoFunction(env *zygo.Zlisp, name string, args []zygo.Sexp) (zygo.Sexp, error) {

	// many parameters, treat as key:value pairs in the hash/record.
	return zygo.ConstructorFunction("msgmap")(env, "msgmap", append([]zygo.Sexp{&zygo.SexpStr{S: name}}, zygo.MakeList(args)))

}

func NewRpcConfigFromSexpString(sexp string, env *zygo.Zlisp) (cfg *Config, err error) {

	if env == nil {
		env = zygo.NewZlisp()
		env.StandardSetup()
		RegisterConfigZygoConstructor(env)
		defer env.Close()
	}

	mycfg, err := env.EvalString(sexp)
	panicOn(err)
	//mystr := mycfg.SexpString(nil)

	_, err = zygo.ToGoFunction(env, "togo", []zygo.Sexp{mycfg})
	panicOn(err)
	cfg = mycfg.(*zygo.SexpHash).GoShadowStruct.(*Config)
	return
}

// test helper
func newConfigReadingZygoEnv() (env *zygo.Zlisp) {
	env = zygo.NewZlisp()
	env.StandardSetup()
	RegisterConfigZygoConstructor(env)
	return
}
