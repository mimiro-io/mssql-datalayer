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
	date := "GETDATE()-1"
	data, err := base64.StdEncoding.DecodeString(q.Request.Since)
	if err == nil {
		dt, _ := time.Parse(time.RFC3339, string(data))
		date = fmt.Sprintf("DATETIMEFROMPARTS( %d, %d, %d, %d, %d, %d, 0)",
			dt.Year(), dt.Month(), dt.Day(), dt.Hour(), dt.Minute(), dt.Second())
	}
	schema := "dbo"
	if q.TableDef.Config != nil && q.TableDef.Config.Schema != nil {
		schema = *q.TableDef.Config.Schema
	}
	query := fmt.Sprintf(`
		DECLARE @begin_time DATETIME, @end_time DATETIME, @begin_lsn BINARY(10), @end_lsn BINARY(10);
		SELECT @begin_time = %s, @end_time = GETDATE();
		SELECT @begin_lsn = sys.fn_cdc_map_time_to_lsn('smallest greater than', @begin_time);
		SELECT @end_lsn = sys.fn_cdc_map_time_to_lsn('largest less than or equal', @end_time);
		SELECT t.* FROM [cdc].[%s_%s_CT] AS t WHERE (t.__$start_lsn <= @end_lsn) AND (t.__$start_lsn >= @begin_lsn);
		`, date, schema, q.TableDef.TableName)

	return query
}