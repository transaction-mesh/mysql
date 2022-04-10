package mysql

import (
	"database/sql/driver"
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
	"time"
)

import (
	"github.com/gotrx/mysql/schema"
)

var (
	DBName         = "gotest"
	deleteSQL      = "DELETE FROM s1 WHERE id = 1;\nDELETE FROM s1 WHERE id = 2;\nDELETE FROM s1;"
	tableName      = "table_update_executor_test"
	multiUpdateSQL = "update table_update_executor_test set name = 'WILL' where id = 1;\nupdate table_update_executor_test set name = 'WILL2' where id = 2"
)

func init() {
	InitTableMetaCache(DBName)
	tableMeta := schema.TableMeta{
		TableName: tableName,
		Columns:   []string{"id", "name", "age"},
		AllColumns: map[string]schema.ColumnMeta{
			"id": {
				TableName:  DBName,
				ColumnName: "id",
			},
		},
		AllIndexes: map[string]schema.IndexMeta{
			"id": {
				ColumnName: "id",
				IndexType:  schema.IndexType_PRIMARY,
			},
		},
	}
	GetTableMetaCache(DBName).addCache(fmt.Sprintf("%s.%s", DBName, tableName), tableMeta)
}

func TestGetTableName(t *testing.T) {
	par := parser.New()
	sql, _ := par.ParseOneStmt("delete from user", "", "")
	tableName := GetTableName(sql.(ast.DMLNode))
	assert.Equal(t, tableName, "`user`")

	sql, _ = par.ParseOneStmt("select * from user", "", "")
	tableName = GetTableName(sql.(ast.DMLNode))
	assert.Equal(t, tableName, "`user`")

	sql, _ = par.ParseOneStmt("update user set user_name = 'starfish'", "", "")
	tableName = GetTableName(sql.(ast.DMLNode))
	assert.Equal(t, tableName, "`user`")

	sql, _ = par.ParseOneStmt("insert into user(id,username) values(1,'starfish')", "", "")
	tableName = GetTableName(sql.(ast.DMLNode))
	assert.Equal(t, tableName, "`user`")
}

func TestGroupStmtByTableName(t *testing.T) {
	par := parser.New()
	stmts, warns, err := par.Parse(deleteSQL, "", "")
	if err != nil {
		t.Error(err)
	}
	if warns != nil {
		t.Error(warns)
	}
	var stmtsRet = make([]*ast.DMLNode, 0)
	for _, stmt := range stmts {
		x := stmt.(ast.DMLNode)
		stmtsRet = append(stmtsRet, &x)
	}
	result := groupStmtByTableName(stmtsRet)
	fmt.Printf("999 %d", len(result))
}

//Multi
func TestMultiExecutor(t *testing.T) {
	parser := parser.New()
	acts, _, _ := parser.Parse(deleteSQL, "", "")

	var stmts []*ast.DMLNode

	for _, act := range acts {
		node := act.(ast.DMLNode)
		stmts = append(stmts, &node)
	}

	_, mc := newRWMockConn(1)
	mc.cfg.User = "root"
	mc.cfg.Passwd = "123456"
	mc.cfg.DBName = DBName

	args := []driver.Value{
		int64(42424242),
		float64(math.Pi),
		false,
		time.Unix(1423411542, 807015000),
		[]byte("bytes containing special chars ' \" \a \x00"),
		"string containing special chars ' \" \a \x00",
	}

	base := BaseExecutor{mc, multiUpdateSQL, args}
	baseExecutor := multiExecutor{
		BaseExecutor: base,
		stmts:        stmts,
	}

	tableRes, _ := baseExecutor.BeforeImage()
	assert.NotEmpty(t, tableRes)
}
