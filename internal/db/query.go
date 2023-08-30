package db

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/mimiro-io/mssqldatalayer/internal/conf"
)

type DatasetRequest struct {
	DatasetName string
	Since       string
	Limit       int64
}

type TableQuery interface {
	BuildQuery() string
}

type FullQuery struct {
	Datalayer *conf.Datalayer
	Request   DatasetRequest
	TableDef  *conf.TableMapping
}

func NewQuery(request DatasetRequest, tableDef *conf.TableMapping, datalayer *conf.Datalayer) TableQuery {
	if strings.Contains(tableDef.CustomQuery, "{{ since }}") {
		return CustomSinceQuery{
			Datalayer: datalayer,
			Request:   request,
			TableDef:  tableDef,
		}
	} else if tableDef.CDCEnabled && request.Since != "" {
		return CDCQuery{
			Datalayer: datalayer,
			Request:   request,
			TableDef:  tableDef,
		}
	} else {
		return FullQuery{
			Datalayer: datalayer,
			Request:   request,
			TableDef:  tableDef,
		}
	}
}

func (q FullQuery) BuildQuery() string {
	schema := q.Datalayer.GetSchema(q.TableDef)
	//tableName := fmt.Sprintf("[%s].[%s]", schema, q.TableDef.TableName)
	tableName := fmt.Sprintf("%s", q.TableDef.TableName)
	if schema != "" {
		tableName = fmt.Sprintf("[%s].[%s]", schema, q.TableDef.TableName)
	}

	limit := ""
	if q.Request.Limit > 0 {
		limit = fmt.Sprintf(" TOP %d ", q.Request.Limit)
	}
	query := fmt.Sprintf("SELECT %s * FROM %s", limit, tableName)
	if q.TableDef.CustomQuery != "" {
		query = fmt.Sprintf(q.TableDef.CustomQuery, limit)
	}

	return query
}

type CustomSinceQuery struct {
	Datalayer *conf.Datalayer
	Request   DatasetRequest
	TableDef  *conf.TableMapping
}

func (q CustomSinceQuery) BuildQuery() string {
	date := fmt.Sprintf("DATETIMEFROMPARTS( %d, %d, %d, %d, %d, %d, %d)",
		1970, 01, 01, 00, 00, 00, 000)
	data, err := base64.StdEncoding.DecodeString(q.Request.Since)
	if err == nil && string(data) != "" {
		dt, _ := time.Parse(time.RFC3339, string(data))
		date = fmt.Sprintf("DATETIMEFROMPARTS( %d, %d, %d, %d, %d, %d, %d)",
			dt.Year(), dt.Month(), dt.Day(), dt.Hour(), dt.Minute(), dt.Second(), dt.Nanosecond()/1000000)
	}

	replaceQuery := strings.Replace(q.TableDef.CustomQuery, "{{ since }}", date, 1)
	query := replaceQuery

	return query
}

type CDCQuery struct {
	Datalayer *conf.Datalayer
	Request   DatasetRequest
	TableDef  *conf.TableMapping
}

func (q CDCQuery) BuildQuery() string {
	schema := "dbo"
	if q.TableDef.Config != nil && q.TableDef.Config.Schema != nil {
		schema = *q.TableDef.Config.Schema
	}

	lastLsn := fmt.Sprintf("sys.fn_cdc_get_min_lsn('%s_%s')", schema, q.TableDef.TableName)
	data, err := base64.RawURLEncoding.DecodeString(q.Request.Since)
	if err == nil && strings.HasPrefix(string(data), "0x") && len(data) == 22 {
		lastLsn = fmt.Sprintf("CONVERT(binary(10), %s)", string(data))
	}

	query := fmt.Sprintf(`
		DECLARE @from_lsn binary(10), @to_lsn binary(10), @last_lsn binary(10);
		SET @last_lsn = %s;
		SET @from_lsn = sys.fn_cdc_increment_lsn(@last_lsn);
		SET @to_lsn = sys.fn_cdc_get_max_lsn();
		SELECT * from cdc.fn_cdc_get_all_changes_%s_%s ( @from_lsn, @to_lsn, 'all' );
`, lastLsn, schema, q.TableDef.TableName)
	return query
}