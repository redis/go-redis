package redis

import (
	"context"
	"strconv"
)

type GraphCmdable interface {
	GraphQuery(ctx context.Context, key, query string) *GraphCmd
}

// GraphQuery executes a query on a graph, exec: GRAPH.QUERY key query
func (c cmdable) GraphQuery(ctx context.Context, key, query string) *GraphCmd {
	cmd := NewGraphCmd(ctx, "GRAPH.QUERY", key, query)
	_ = c(ctx, cmd)
	return cmd
}

// ----------------------------------------------------------------------------

type GraphValue interface {
	IsNil() bool
	String() string
	Int() int
	Bool() bool
	Float64() float64
	Node() (*GraphNode, bool)
	Edge() (*GraphEdge, bool)
}

type (
	graphDataType int
	graphRowType  int
)

const (
	graphInteger graphDataType = iota + 1 // int (graph int)
	graphNil                              // nil (graph nil/null)
	graphString                           // string (graph string/boolean/double)
)

const (
	graphResultBasic graphRowType = iota + 1 // int/nil/string
	graphResultNode                          // node(3), id +labels +properties
	graphResultEdge                          // edge(5), id + type + src_node + dest_node + properties
)

type GraphResult struct {
	noResult bool
	text     []string
	field    []string
	rows     [][]*graphRow
	err      error
}

// Message is used to obtain statistical information about the execution of graph statements.
func (g *GraphResult) Message() []string {
	return g.text
}

// IsResult indicates whether the statement has a result set response.
// For example, a MATCH statement always has a result set (regardless of the number of rows in the response),
// while a CREATE statement does not.
func (g *GraphResult) IsResult() bool {
	return !g.noResult
}

// Error return exec error
func (g *GraphResult) Error() error {
	return g.err
}

// Field returns the fields in the query result set.
// If the statement has no result set (IsResult() == false), it returns nil.
func (g *GraphResult) Field() []string {
	return g.field
}

// Len return the number of rows in the result set.
func (g *GraphResult) Len() int {
	return len(g.rows)
}

// Row read the first row of data in the result set. If there is no data response, return redis.Nil.
func (g *GraphResult) Row() (map[string]GraphValue, error) {
	if g.err != nil {
		return nil, g.err
	}
	if g.noResult || len(g.field) == 0 || len(g.rows) == 0 || len(g.rows[0]) == 0 {
		return nil, Nil
	}
	row := make(map[string]GraphValue, len(g.field))
	for i, f := range g.field {
		row[f] = g.rows[0][i]
	}
	return row, nil
}

// Rows return all result set responses, If there is no data response, return redis.Nil.
func (g *GraphResult) Rows() ([]map[string]GraphValue, error) {
	if g.err != nil {
		return nil, g.err
	}
	if g.noResult || len(g.field) == 0 || len(g.rows) == 0 || len(g.rows[0]) == 0 {
		return nil, Nil
	}
	rows := make([]map[string]GraphValue, 0, len(g.rows))
	for i := 0; i < len(g.rows); i++ {
		row := make(map[string]GraphValue, len(g.field))
		for x, f := range g.field {
			row[f] = g.rows[i][x]
		}
		rows = append(rows, row)
	}
	return rows, nil
}

type graphRow struct {
	typ   graphRowType
	basic GraphData
	node  GraphNode
	edge  GraphEdge
}

func (g *graphRow) IsNil() bool              { return g.basic.IsNil() }
func (g *graphRow) String() string           { return g.basic.String() }
func (g *graphRow) Int() int                 { return g.basic.Int() }
func (g *graphRow) Bool() bool               { return g.basic.Bool() }
func (g *graphRow) Float64() float64         { return g.basic.Float64() }
func (g *graphRow) Node() (*GraphNode, bool) { return &g.node, g.typ == graphResultNode }
func (g *graphRow) Edge() (*GraphEdge, bool) { return &g.edge, g.typ == graphResultEdge }

type GraphData struct {
	typ        graphDataType
	integerVal int64
	stringVal  string
}

func (d GraphData) IsNil() bool {
	return d.typ == graphNil
}

func (d GraphData) String() string {
	switch d.typ {
	case graphInteger:
		return strconv.FormatInt(d.integerVal, 10)
	case graphNil:
		return ""
	case graphString:
		return d.stringVal
	default:
		return ""
	}
}

func (d GraphData) Int() int {
	switch d.typ {
	case graphInteger:
		return int(d.integerVal)
	case graphString:
		n, _ := strconv.Atoi(d.stringVal)
		return n
	default:
		return 0
	}
}

func (d GraphData) Bool() bool {
	switch d.typ {
	case graphInteger:
		return d.integerVal != 0
	case graphNil:
		return false
	case graphString:
		return d.stringVal == "true"
	default:
		return false
	}
}

func (d GraphData) Float64() float64 {
	if d.typ == graphInteger {
		return float64(d.integerVal)
	}
	if d.typ == graphString {
		v, _ := strconv.ParseFloat(d.stringVal, 64)
		return v
	}
	return 0
}

type GraphNode struct {
	ID         int64
	Labels     []string
	Properties map[string]GraphData
}

type GraphEdge struct {
	ID         int64
	Typ        string
	SrcNode    int64
	DstNode    int64
	Properties map[string]GraphData
}
