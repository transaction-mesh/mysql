package mysql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"time"
)

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/transaction-mesh/starfish/pkg/base/meta"
	"github.com/transaction-mesh/starfish/pkg/util/mysql"
	sql2 "github.com/transaction-mesh/starfish/pkg/util/sql"
)

import (
	"github.com/transaction-mesh/mysql/schema"
)

// IExecutor : basic executor
type IExecutor interface {
	Execute() (driver.Result, error)
	BeforeImage() (*schema.TableRecords, error)
}

type BaseExecutor struct {
	mc          *mysqlConn
	originalSQL string
	args        []driver.Value
}

func (executor *BaseExecutor) getTableMeta(tableName string) (schema.TableMeta, error) {
	tableMetaCache := GetTableMetaCache(executor.mc.cfg.DBName)
	return tableMetaCache.GetTableMeta(executor.mc, tableName)
}

func (executor *BaseExecutor) buildLockKey(lockKeyRecords *schema.TableRecords) string {
	if lockKeyRecords.Rows == nil || len(lockKeyRecords.Rows) == 0 {
		return ""
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, lockKeyRecords.TableName)
	fmt.Fprint(&sb, ":")
	fields := lockKeyRecords.PKFields()
	length := len(fields)
	for i, field := range fields {
		fmt.Fprint(&sb, field.Value)
		if i < length-1 {
			fmt.Fprint(&sb, ",")
		}
	}
	return sb.String()
}

func (executor *BaseExecutor) buildUndoItem(sqlType SQLType, tableName string, beforeImage, afterImage *schema.TableRecords) *sqlUndoLog {
	sqlUndoLog := &sqlUndoLog{
		SqlType:     sqlType,
		TableName:   tableName,
		BeforeImage: beforeImage,
		AfterImage:  afterImage,
	}
	return sqlUndoLog
}

func (executor *BaseExecutor) buildRecords(meta schema.TableMeta, rows driver.Rows) *schema.TableRecords {
	resultSet := rows.(*binaryRows)
	records := schema.NewTableRecords(meta)
	columns := resultSet.Columns()
	rs := make([]*schema.Row, 0)

	values := make([]driver.Value, len(columns))

	for {
		err := resultSet.Next(values)
		if err != nil {
			break
		}

		fields := make([]*schema.Field, 0, len(columns))
		for i, col := range columns {
			filed := &schema.Field{
				Name:  col,
				Type:  meta.AllColumns[col].DataType,
				Value: values[i],
			}
			switch v := values[i].(type) {
			case []uint8:
				dst := make([]uint8, len(v))
				copy(dst, v)
				filed.Value = dst
			}
			if strings.ToLower(col) == strings.ToLower(meta.GetPKName()) {
				filed.KeyType = schema.PRIMARY_KEY
			}
			fields = append(fields, filed)
		}
		row := &schema.Row{Fields: fields}
		rs = append(rs, row)
	}
	records.Rows = rs
	return records
}

func (executor *BaseExecutor) buildWhereCondition(where ast.ExprNode) string {
	var sb strings.Builder
	where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

type insertExecutor struct {
	BaseExecutor
	stmt *ast.InsertStmt
}

type deleteExecutor struct {
	BaseExecutor
	stmt *ast.DeleteStmt
}

type selectForUpdateExecutor struct {
	BaseExecutor
	stmt *ast.SelectStmt
}

type updateExecutor struct {
	BaseExecutor
	stmt *ast.UpdateStmt
}

type multiExecutor struct {
	BaseExecutor
	stmts     []*ast.DMLNode
	groupStmt map[string][]ast.DMLNode
}

type multiUpdateExecutor struct {
	BaseExecutor
	stmts []*ast.UpdateStmt
}

func (executor *multiUpdateExecutor) Execute() (driver.Result, error) {
	//TODO implement me
	panic("implement me")
}

type multiDeleteExecutor struct {
	BaseExecutor
	stmts []*ast.DeleteStmt
}

func (executor *multiDeleteExecutor) Execute() (driver.Result, error) {
	//TODO implement me
	panic("implement me")
}

func (executor *insertExecutor) GetInsertColumns() []string {
	result := make([]string, 0)
	for _, col := range executor.stmt.Columns {
		result = append(result, col.Name.String())
	}
	return result
}

func (executor *insertExecutor) Execute() (driver.Result, error) {
	beforeImage, err := executor.BeforeImage()
	if err != nil {
		return nil, err
	}
	result, err := executor.mc.execAlways(executor.originalSQL, executor.args)
	if err != nil {
		return result, err
	}

	afterImage, err := executor.AfterImage(result)

	if err != nil {
		return nil, err
	}
	executor.PrepareUndoLog(beforeImage, afterImage)
	return result, err
}

func (executor *insertExecutor) PrepareUndoLog(beforeImage, afterImage *schema.TableRecords) {
	if len(afterImage.Rows) == 0 {
		return
	}

	var lockKeyRecords = afterImage

	lockKeys := executor.buildLockKey(lockKeyRecords)
	executor.mc.ctx.AppendLockKey(lockKeys)

	sqlUndoLog := executor.buildUndoItem(SQLType_INSERT, GetTableName(executor.stmt), beforeImage, afterImage)
	executor.mc.ctx.AppendUndoItem(sqlUndoLog)
}

func (executor *insertExecutor) BeforeImage() (*schema.TableRecords, error) {
	return nil, nil
}

func (executor *insertExecutor) AfterImage(result sql.Result) (*schema.TableRecords, error) {
	var afterImage *schema.TableRecords
	var err error
	pkValues := executor.getPKValuesByColumn()
	if executor.getPKIndex() >= 0 {
		afterImage, err = executor.BuildTableRecords(pkValues)
	} else {
		pk, _ := result.LastInsertId()
		afterImage, err = executor.BuildTableRecords([]driver.Value{pk})
	}
	if err != nil {
		return nil, err
	}
	return afterImage, nil
}

func (executor *insertExecutor) BuildTableRecords(pkValues []driver.Value) (*schema.TableRecords, error) {
	tableMeta, err := executor.getTableMeta(GetTableName(executor.stmt))
	if err != nil {
		return nil, err
	}
	var sb strings.Builder
	fmt.Fprint(&sb, "SELECT ")
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, column := range tableMeta.Columns {
		fmt.Fprint(&sb, CheckAndReplace(column))
		i = i + 1
		if i < columnCount {
			fmt.Fprint(&sb, ",")
		} else {
			fmt.Fprint(&sb, " ")
		}
	}
	fmt.Fprintf(&sb, "FROM %s ", GetTableName(executor.stmt))
	fmt.Fprintf(&sb, " WHERE `%s` IN ", tableMeta.GetPKName())
	fmt.Fprint(&sb, AppendInParam(len(pkValues)))

	rows, err := executor.mc.prepareQuery(sb.String(), pkValues)
	if err != nil {
		return nil, err
	}
	return executor.buildRecords(tableMeta, rows), nil
}

func (executor *insertExecutor) getPKValuesByColumn() []driver.Value {
	pkValues := make([]driver.Value, 0)
	columnLen := executor.getColumnLen()
	pkIndex := executor.getPKIndex()
	for i, value := range executor.args {
		if i%columnLen == pkIndex {
			pkValues = append(pkValues, value)
		}
	}
	return pkValues
}

func (executor *insertExecutor) getPKIndex() int {
	insertColumns := executor.GetInsertColumns()
	tableMeta, _ := executor.getTableMeta(GetTableName(executor.stmt))

	if insertColumns != nil && len(insertColumns) > 0 {
		for i, columnName := range insertColumns {
			if strings.EqualFold(tableMeta.GetPKName(), columnName) {
				return i
			}
		}
	} else {
		allColumns := tableMeta.Columns
		var idx = 0
		for _, column := range allColumns {
			if strings.EqualFold(tableMeta.GetPKName(), column) {
				return idx
			}
			idx = idx + 1
		}
	}
	return -1
}

func (executor *insertExecutor) getColumnLen() int {
	insertColumns := executor.GetInsertColumns()
	if insertColumns != nil {
		return len(insertColumns)
	}
	tableMeta, _ := executor.getTableMeta(GetTableName(executor.stmt))

	return len(tableMeta.Columns)
}

func (executor *deleteExecutor) Execute() (driver.Result, error) {
	beforeImage, err := executor.BeforeImage()
	if err != nil {
		return nil, err
	}
	result, err := executor.mc.execAlways(executor.originalSQL, executor.args)
	if err != nil {
		return result, err
	}
	afterImage, err := executor.AfterImage()
	if err != nil {
		return nil, err
	}
	executor.PrepareUndoLog(beforeImage, afterImage)
	return result, err
}

func (executor *deleteExecutor) PrepareUndoLog(beforeImage, afterImage *schema.TableRecords) {
	if len(beforeImage.Rows) == 0 {
		return
	}

	var lockKeyRecords = beforeImage

	lockKeys := executor.buildLockKey(lockKeyRecords)
	executor.mc.ctx.AppendLockKey(lockKeys)

	sqlUndoLog := executor.buildUndoItem(SQLType_DELETE, GetTableName(executor.stmt), beforeImage, afterImage)
	executor.mc.ctx.AppendUndoItem(sqlUndoLog)
}

func (executor *deleteExecutor) BeforeImage() (*schema.TableRecords, error) {
	tableMeta, err := executor.getTableMeta(GetTableName(executor.stmt))
	if err != nil {
		return nil, err
	}
	return executor.buildTableRecords(tableMeta)
}

func (executor *deleteExecutor) AfterImage() (*schema.TableRecords, error) {
	return nil, nil
}

func (executor *deleteExecutor) buildTableRecords(tableMeta schema.TableMeta) (*schema.TableRecords, error) {
	rows, err := executor.mc.prepareQuery(executor.buildBeforeImageSql(tableMeta), executor.args)
	if err != nil {
		return nil, err
	}
	return executor.buildRecords(tableMeta, rows), nil
}

func (executor *deleteExecutor) buildBeforeImageSql(tableMeta schema.TableMeta) string {
	var b strings.Builder
	fmt.Fprint(&b, "SELECT ")
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, column := range tableMeta.Columns {
		fmt.Fprint(&b, mysql.CheckAndReplace(column))
		i = i + 1
		if i < columnCount {
			fmt.Fprint(&b, ",")
		} else {
			fmt.Fprint(&b, " ")
		}
	}
	fmt.Fprintf(&b, " FROM %s WHERE ", GetTableName(executor.stmt))
	fmt.Fprint(&b, executor.buildWhereCondition(executor.stmt.Where))
	fmt.Fprint(&b, " FOR UPDATE")
	return b.String()
}

func (executor *selectForUpdateExecutor) Execute(lockRetryInterval time.Duration, lockRetryTimes int) (driver.Rows, error) {
	tableMeta, err := executor.getTableMeta(GetTableName(executor.stmt))
	if err != nil {
		return nil, err
	}
	rows, err := executor.mc.prepareQuery(executor.originalSQL, executor.args)
	if err != nil {
		return nil, err
	}
	selectPKRows := executor.buildRecords(tableMeta, rows)
	lockKeys := executor.buildLockKey(selectPKRows)
	if lockKeys == "" {
		return rows, err
	} else {
		if executor.mc.ctx.xid != "" {
			var lockable bool
			var err error
			for i := 0; i < lockRetryTimes; i++ {
				lockable, err = dataSourceManager.LockQuery(meta.BranchTypeAT,
					executor.mc.cfg.DBName, executor.mc.ctx.xid, lockKeys)
				if lockable && err == nil {
					break
				}
				time.Sleep(lockRetryInterval)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return rows, err
}

func (executor *updateExecutor) GetUpdateColumns() []string {
	columns := make([]string, 0)
	for _, assignment := range executor.stmt.List {
		columns = append(columns, assignment.Column.Name.String())
	}
	return columns
}

func (executor *updateExecutor) Execute() (driver.Result, error) {
	beforeImage, err := executor.BeforeImage()
	if err != nil {
		return nil, err
	}
	result, err := executor.mc.execAlways(executor.originalSQL, executor.args)
	if err != nil {
		return result, err
	}
	afterImage, err := executor.AfterImage(beforeImage)
	if err != nil {
		return nil, err
	}
	executor.PrepareUndoLog(beforeImage, afterImage)
	return result, err
}

func (executor *updateExecutor) PrepareUndoLog(beforeImage, afterImage *schema.TableRecords) {
	if len(beforeImage.Rows) == 0 &&
		(afterImage == nil || len(afterImage.Rows) == 0) {
		return
	}

	var lockKeyRecords = afterImage

	lockKeys := executor.buildLockKey(lockKeyRecords)
	executor.mc.ctx.AppendLockKey(lockKeys)

	sqlUndoLog := executor.buildUndoItem(SQLType_UPDATE, GetTableName(executor.stmt), beforeImage, afterImage)
	executor.mc.ctx.AppendUndoItem(sqlUndoLog)
}

func (executor *updateExecutor) BeforeImage() (*schema.TableRecords, error) {
	tableMeta, err := executor.getTableMeta(GetTableName(executor.stmt))
	if err != nil {
		return nil, err
	}
	return executor.buildTableRecords(tableMeta)
}

func (executor *updateExecutor) AfterImage(beforeImage *schema.TableRecords) (*schema.TableRecords, error) {
	if beforeImage.Rows == nil || len(beforeImage.Rows) == 0 {
		return nil, nil
	}

	tableMeta, err := executor.getTableMeta(GetTableName(executor.stmt))
	if err != nil {
		return nil, err
	}
	afterImageSql := executor.buildAfterImageSql(tableMeta, beforeImage)
	var args = make([]driver.Value, 0)
	for _, field := range beforeImage.PKFields() {
		args = append(args, field.Value)
	}
	rows, err := executor.mc.prepareQuery(afterImageSql, args)
	if err != nil {
		return nil, err
	}
	return executor.buildRecords(tableMeta, rows), nil
}

func (executor *updateExecutor) buildAfterImageSql(tableMeta schema.TableMeta, beforeImage *schema.TableRecords) string {
	var b strings.Builder
	fmt.Fprint(&b, "SELECT ")
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, columnName := range tableMeta.Columns {
		fmt.Fprint(&b, mysql.CheckAndReplace(columnName))
		i = i + 1
		if i < columnCount {
			fmt.Fprint(&b, ",")
		} else {
			fmt.Fprint(&b, " ")
		}
	}
	fmt.Fprintf(&b, " FROM %s ", GetTableName(executor.stmt))
	fmt.Fprintf(&b, "WHERE `%s` IN", tableMeta.GetPKName())
	fmt.Fprint(&b, AppendInParam(len(beforeImage.PKFields())))
	return b.String()
}

func (executor *updateExecutor) buildTableRecords(tableMeta schema.TableMeta) (*schema.TableRecords, error) {
	sql := executor.buildBeforeImageSql(tableMeta)
	argsCount := strings.Count(sql, "?")
	rows, err := executor.mc.prepareQuery(sql, executor.args[len(executor.args)-argsCount:])
	if err != nil {
		return nil, err
	}
	return executor.buildRecords(tableMeta, rows), nil
}

func (executor *updateExecutor) buildBeforeImageSql(tableMeta schema.TableMeta) string {
	var b strings.Builder
	fmt.Fprint(&b, "SELECT ")
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, column := range tableMeta.Columns {
		fmt.Fprint(&b, mysql.CheckAndReplace(column))
		i = i + 1
		if i != columnCount {
			fmt.Fprint(&b, ",")
		} else {
			fmt.Fprint(&b, " ")
		}
	}
	fmt.Fprintf(&b, " FROM %s WHERE ", GetTableName(executor.stmt))
	fmt.Fprint(&b, executor.buildWhereCondition(executor.stmt.Where))
	fmt.Fprint(&b, " FOR UPDATE")
	return b.String()
}

func (executor *multiExecutor) BeforeImage() (*schema.TableRecords, error) {
	executor.groupStmt = groupStmtByTableName(executor.stmts)
	for _, stmts := range executor.groupStmt {
		tmpStmt := stmts[0]

		var iExec IExecutor
		switch tmpStmt.(type) {
		case *ast.DeleteStmt:
			var tmpStmts = make([]*ast.DeleteStmt, 0)
			for _, stmt := range stmts {
				tmpStmts = append(tmpStmts, stmt.(*ast.DeleteStmt))
			}
			iExec = &multiDeleteExecutor{
				BaseExecutor: executor.BaseExecutor,
				stmts:        tmpStmts,
			}
		case *ast.UpdateStmt:
			var tmpStmts = make([]*ast.UpdateStmt, 0)
			for _, stmt := range stmts {
				tmpStmts = append(tmpStmts, stmt.(*ast.UpdateStmt))
			}
			iExec = &multiUpdateExecutor{
				BaseExecutor: executor.BaseExecutor,
				stmts:        tmpStmts,
			}
		}
		iExec.BeforeImage()
	}
	return nil, nil
}

func (executor *multiExecutor) AfterImage(beforeImage *schema.TableRecords) (*schema.TableRecords, error) {
	for _, stmts := range executor.groupStmt {
		tmpStmt := stmts[0]

		var _ IExecutor
		switch tmpStmt.(type) {
		case *ast.DeleteStmt:
			var tmpStmts = make([]*ast.DeleteStmt, 0)
			for _, stmt := range stmts {
				tmpStmts = append(tmpStmts, stmt.(*ast.DeleteStmt))
			}
			_ = &multiDeleteExecutor{
				BaseExecutor: executor.BaseExecutor,
				stmts:        tmpStmts,
			}
		case *ast.UpdateStmt:
			var tmpStmts = make([]*ast.UpdateStmt, 0)
			for _, stmt := range stmts {
				tmpStmts = append(tmpStmts, stmt.(*ast.UpdateStmt))
			}
			_ = &multiUpdateExecutor{
				BaseExecutor: executor.BaseExecutor,
				stmts:        tmpStmts,
			}
		}
		// TODO afterImage
		//iExec.AfterImage()
	}
	return nil, nil
}

func (executor *multiExecutor) Execute() (driver.Result, error) {
	return nil, nil
}

// group
func groupStmtByTableName(stmts []*ast.DMLNode) map[string][]ast.DMLNode {
	var groupMap = make(map[string][]ast.DMLNode, 1)
	for _, stmt := range stmts {
		var tableName = GetTableName(*stmt)
		tmpArr, ok := groupMap[tableName]
		if ok {
			tmpArr = append(tmpArr, *stmt)
			groupMap[tableName] = tmpArr
		} else {
			var arr = make([]ast.DMLNode, 0)
			arr = append(arr, *stmt)
			groupMap[tableName] = arr
		}

	}
	return groupMap
}

func (executor *multiDeleteExecutor) buildTableRecords(tableMeta schema.TableMeta) (*schema.TableRecords, error) {
	sql := executor.buildBeforeImageSql(tableMeta)
	argsCount := strings.Count(sql, "?")
	rows, err := executor.mc.prepareQuery(sql, executor.args[len(executor.args)-argsCount:])
	if err != nil {
		return nil, err
	}
	return executor.buildRecords(tableMeta, rows), nil
}

func (executor *multiDeleteExecutor) buildBeforeImageSql(tableMeta schema.TableMeta) string {
	var suffix strings.Builder
	suffix.WriteString(" FROM ")
	suffix.WriteString(GetTableName(executor.stmts[0]))
	whereCondition := executor.buildWhereCondition(executor.stmts[0].Where)
	if len(whereCondition) > 0 {
		suffix.WriteString(" WHERE ")
		suffix.WriteString(whereCondition)
	}
	suffix.WriteString(" FOR UPDATE")

	var selectSql strings.Builder
	selectSql.WriteString("SELECT *")
	selectSql.WriteString(suffix.String())
	return selectSql.String()
}

func (executor *multiDeleteExecutor) BeforeImage() (*schema.TableRecords, error) {
	if len(executor.stmts) == 1 {
		exec := &deleteExecutor{
			stmt: executor.stmts[0],
		}
		exec.mc = executor.mc
		exec.originalSQL = executor.originalSQL
		exec.args = executor.args
		return exec.BeforeImage()
	}
	tableMeta, err := executor.getTableMeta(GetTableName(executor.stmts[0]))
	if err != nil {
		return nil, err
	}
	return executor.buildTableRecords(tableMeta)
}

func (executor *multiUpdateExecutor) GetWhereCondition() string {
	var whereCondition strings.Builder
	var whereConditionTmp strings.Builder
	for _, stmtTmp := range executor.stmts {
		whereConditionTmp.Reset()
		if stmtTmp.Where == nil {
			break
		}
		stmtTmp.Where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &whereConditionTmp))
		if whereCondition.Len() > 0 {
			whereCondition.WriteString(" OR ")
		}
		whereCondition.WriteString(whereConditionTmp.String())
	}
	return whereCondition.String()
}

func (executor *multiUpdateExecutor) buildTableRecords(tableMeta schema.TableMeta) (*schema.TableRecords, error) {
	sql := executor.buildBeforeImageSql(tableMeta)
	argsCount := strings.Count(sql, "?")
	rows, err := executor.mc.prepareQuery(sql, executor.args[len(executor.args)-argsCount:])
	if err != nil {
		return nil, err
	}
	return executor.buildRecords(tableMeta, rows), nil
}

func (executor *multiUpdateExecutor) buildBeforeImageSql(tableMeta schema.TableMeta) string {
	var suffix strings.Builder
	suffix.WriteString(" FROM ")
	suffix.WriteString(GetTableName(executor.stmts[0]))
	whereCondition := executor.GetWhereCondition()
	if len(whereCondition) > 0 {
		suffix.WriteString(" WHERE ")
		suffix.WriteString(whereCondition)
	}
	suffix.WriteString(" FOR UPDATE")

	var selectSql strings.Builder
	selectSql.WriteString("SELECT *")
	selectSql.WriteString(suffix.String())
	return selectSql.String()
}

func (executor *multiUpdateExecutor) BeforeImage() (*schema.TableRecords, error) {
	if len(executor.stmts) == 1 {
		exec := updateExecutor{
			stmt: executor.stmts[0],
		}
		exec.mc = executor.mc
		exec.originalSQL = executor.originalSQL
		exec.args = executor.args
		return exec.BeforeImage()
	}
	tableMeta, err := executor.getTableMeta(GetTableName(executor.stmts[0]))
	if err != nil {
		return nil, err
	}
	return executor.buildTableRecords(tableMeta)
}

func (executor *multiUpdateExecutor) AfterImage(beforeImage *schema.TableRecords) (*schema.TableRecords, error) {
	if len(executor.stmts) == 1 {
		exec := updateExecutor{
			stmt: executor.stmts[0],
		}
		exec.mc = executor.mc
		exec.originalSQL = executor.originalSQL
		exec.args = executor.args
		return exec.AfterImage(beforeImage)
	}
	tableMeta, err := executor.getTableMeta(GetTableName(executor.stmts[0]))
	if err != nil {
		return nil, err
	}
	return executor.buildTableRecords(tableMeta)
}

func AppendInParam(size int) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "(")
	for i := 0; i < size; i++ {
		fmt.Fprintf(&sb, "?")
		if i < size-1 {
			fmt.Fprint(&sb, ",")
		}
	}
	fmt.Fprintf(&sb, ")")
	return sb.String()
}

func GetTableName(node ast.DMLNode) string {
	var sb strings.Builder
	switch stmt := node.(type) {
	case *ast.InsertStmt:
		stmt.Table.TableRefs.Left.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	case *ast.UpdateStmt:
		stmt.TableRefs.TableRefs.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	case *ast.DeleteStmt:
		stmt.TableRefs.TableRefs.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	case *ast.SelectStmt:
		table := stmt.From.TableRefs.Left.(*ast.TableSource)
		table.Source.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	}
	return sb.String()
}
