package conf

import (
	"fmt"
	"net/url"
	"os"
)

type Datalayer struct {
	Id             string          `json:"id"`
	DatabaseServer string          `json:"databaseServer"`
	BaseUri        string          `json:"baseUri"`
	Database       string          `json:"database"`
	Port           string          `json:"port"`
	Schema         string          `json:"schema"`
	BaseNameSpace  string          `json:"baseNameSpace"`
	User           string          `json:"user"`
	Password       string          `json:"password"`
	Instance       string          `json:"instance"`
	TableMappings  []*TableMapping `json:"tableMappings"`
	PostMappings   []*PostMapping  `json:"postMappings"`
}

type TableMapping struct {
	TableName           string           `json:"tableName"`
	NameSpace           string           `json:"nameSpace"`
	CustomQuery         string           `json:"query"`
	CDCEnabled          bool             `json:"cdcEnabled"`
	SinceColumn         string           `json:"sinceColumn"`
	EntityIdConstructor string           `json:"entityIdConstructor"`
	Types               []string         `json:"types"`
	ColumnMappings      []*ColumnMapping `json:"columnMappings"`
	Config              *TableConfig     `json:"config"`
	Columns             map[string]*ColumnMapping
}

type ColumnMapping struct {
	FieldName         string `json:"fieldName"`
	PropertyName      string `json:"propertyName"`
	IsIdColumn        bool   `json:"isIdColumn"`
	IsReference       bool   `json:"isReference"`
	ReferenceTemplate string `json:"referenceTemplate"`
	IgnoreColumn      bool   `json:"ignoreColumn"`
}

type PostMapping struct {
	DatasetName           string          `json:"datasetName"`
	TableName             string          `json:"tableName"`
	IdColumn              string          `json:"idColumn"`
	Query                 string          `json:"query"`
	Config                *TableConfig    `json:"config"`
	FieldMappings         []*FieldMapping `json:"fieldMappings"`
	NullEmptyColumnValues bool            `json:"nullEmptyColumnValues"`
}

type FieldMapping struct {
	FieldName        string `json:"fieldName"`
	SortOrder        int    `json:"order"`
	ResolveNamespace bool   `json:"resolveNamespace"`
	DataType         string `json:"dataType"`
}

type TableConfig struct {
	DatabaseServer *string         `json:"databaseServer"`
	Database       *string         `json:"database"`
	Port           *string         `json:"port"`
	Schema         *string         `json:"schema"`
	Instance       *string         `json:"instance"`
	User           *VariableGetter `json:"user"`
	Password       *VariableGetter `json:"password"`
}

type VariableGetter struct {
	Type string `json:"type"`
	Key  string `json:"key"`
}

func (v *VariableGetter) GetValue() string {
	// no type supported for the moment
	return os.Getenv(v.Key)
}

func (layer *Datalayer) GetUrl(table *TableMapping) *url.URL {
	database := layer.Database
	port := layer.Port
	server := layer.DatabaseServer
	user := layer.User
	password := layer.Password
	instance := layer.Instance
	if table.Config != nil {
		if table.Config.Database != nil {
			database = *table.Config.Database
		}
		if table.Config.Port != nil {
			port = *table.Config.Port
		}
		if table.Config.DatabaseServer != nil {
			server = *table.Config.DatabaseServer
		}
		if table.Config.User != nil {
			user = table.Config.User.GetValue()
		}
		if table.Config.Password != nil {
			password = table.Config.Password.GetValue()
		}
		if table.Config.Instance != nil {
			instance = *table.Config.Instance
		}
	}

	query := url.Values{}
	query.Add("database", database)
	query.Add("packet size", "32767")
	//query.Add("log", "32")
	u := &url.URL{}
	if instance != "" {
		u = &url.URL{
			Scheme:   "sqlserver",
			User:     url.UserPassword(user, password),
			Host:     server,
			Path:     instance, // if connecting to an instance instead of a port
			RawQuery: query.Encode(),
		}
	} else {
		u = &url.URL{
			Scheme: "sqlserver",
			User:   url.UserPassword(user, password),
			Host:   fmt.Sprintf("%s:%s", server, port),
			//Path:     instance, // if connecting to an instance instead of a port
			RawQuery: query.Encode(),
		}
	}
	return u
}
func (layer *Datalayer) GetPostUrl(table *PostMapping) *url.URL {
	database := layer.Database
	port := layer.Port
	server := layer.DatabaseServer
	user := layer.User
	password := layer.Password
	instance := layer.Instance
	if table.Config != nil {
		if table.Config.Database != nil {
			database = *table.Config.Database
		}
		if table.Config.Port != nil {
			port = *table.Config.Port
		}
		if table.Config.DatabaseServer != nil {
			server = *table.Config.DatabaseServer
		}
		if table.Config.User != nil {
			user = table.Config.User.Key
		}
		if table.Config.Password != nil {
			password = table.Config.Password.Key
		}
		if table.Config.Instance != nil {
			instance = *table.Config.Instance
		}
	}

	query := url.Values{}
	query.Add("database", database)
	query.Add("packet size", "32767")
	//query.Add("log", "32")
	u := &url.URL{}
	if instance != "" {
		u = &url.URL{
			Scheme:   "sqlserver",
			User:     url.UserPassword(user, password),
			Host:     server,
			Path:     instance, // if connecting to an instance instead of a port
			RawQuery: query.Encode(),
		}
	} else {
		u = &url.URL{
			Scheme: "sqlserver",
			User:   url.UserPassword(user, password),
			Host:   fmt.Sprintf("%s:%s", server, port),
			//Path:     instance, // if connecting to an instance instead of a port
			RawQuery: query.Encode(),
		}
	}
	return u
}

func (layer *Datalayer) GetSchema(table *TableMapping) string {
	schema := layer.Schema
	if table.Config != nil {
		if table.Config.Schema != nil {
			schema = *table.Config.Schema
		}
	}
	return schema
}
