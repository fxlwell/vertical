package orm

import (
	vertical_util "vertical/util"
)

type God struct {
	Model      *Ref_Model
	Factory    func() Model
	MasterNode string
	SlaveNode  string
	Table      Table
	Tpls       map[string][]*Expr

	LastStatement *Statement
}

var (
	Expr_star   = E_field("*")
	Exprs_star  = []*Expr{E_field("*")}
	Exprs_empty = []*Expr{}
)

func NewGod(factory func() Model, node string, table Table) *God {
	return &God{
		Model:      NewRefModel(factory()),
		Factory:    factory,
		MasterNode: node,
		SlaveNode:  node,
		Table:      table,
		Tpls:       make(map[string][]*Expr),
	}
}

func NewGod_master_slave(factory func() Model, master_node string, slave_node string, table Table) *God {
	return &God{
		Model:      NewRefModel(factory()),
		Factory:    factory,
		MasterNode: master_node,
		SlaveNode:  slave_node,
		Table:      table,
		Tpls:       make(map[string][]*Expr),
	}
}

func (g *God) Tpl(tpl string, fields ...interface{}) {
	g.Tpls[tpl] = g.args_to_field_exprs(fields...)
}

func (g *God) NewStatement() *Statement {
	return &Statement{
		God: g,
	}
}

func (g *God) Sharding(datas ...interface{}) *Statement {
	stmt := g.NewStatement()
	stmt.TableClause = []*Expr{E_table(g.Table.Name(datas...))}
	return stmt
}

func (g *God) Shardings(datas ...interface{}) []*Statement {
	stmts := []*Statement{}
	for table_name, sharding_data := range Names(g.Table, datas...) {
		stmt := g.NewStatement()
		stmt.TableClause = []*Expr{E_table(table_name)}
		stmt.ShardingData = sharding_data
		stmts = append(stmts, stmt)
	}
	return stmts
}

func (g *God) args_to_field_exprs_with_tpl(args ...interface{}) []*Expr {
	if len(args) == 0 {
		return Exprs_star
	}
	if tpl, ok := args[0].(string); ok {
		if v, ok := g.Tpls[tpl]; ok {
			return v
		}
	}
	return g.args_to_field_exprs(args...)
}
func (g *God) args_to_field_exprs(args ...interface{}) []*Expr {
	if len(args) == 0 {
		return Exprs_star
	}
	exprs := []*Expr{}
	for _, field := range args {
		switch v := field.(type) {
		case string:
			exprs = append(exprs, E_field(v))
		case *Expr:
			exprs = append(exprs, v)
		default:
			vertical_util.Panicf("field type is not string and *Expr")
		}
	}
	return exprs
}
