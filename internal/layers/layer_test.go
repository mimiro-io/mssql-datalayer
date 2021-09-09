package layers

import (
	"reflect"
	"testing"

	"github.com/mimiro-io/mssqldatalayer/internal/conf"
)

func TestLayer_GetDatasetNames(t *testing.T) {
	tm := []*conf.TableMapping{
		{
			TableName: "Table1",
		},
		{
			TableName: "Table2",
		},
	}

	layer := Layer{
		cmgr: &conf.ConfigurationManager{
			Datalayer: &conf.Datalayer{
				TableMappings: tm,
			},
		},
	}

	tableNames := layer.GetDatasetNames()
	check := []string{"Table1", "Table2"}

	if !reflect.DeepEqual(tableNames, check) {
		t.Errorf("%s != %s", tableNames, check)
	}

}

func TestLayer_GetTableDefinition(t *testing.T) {
	tm := []*conf.TableMapping{
		{
			TableName: "Table1",
		},
		{
			TableName: "Table2",
		},
	}
	layer := Layer{
		cmgr: &conf.ConfigurationManager{
			Datalayer: &conf.Datalayer{
				TableMappings: tm,
			},
		},
	}

	tdef := layer.GetTableDefinition("Table1")
	if tdef.TableName != "Table1" {
		t.Errorf("%s != Table1", tdef.TableName)
	}

	tdef = layer.GetTableDefinition("NotPresent")
	if tdef != nil {
		t.Errorf("Definitions should have returned nil, but was %p", tdef)
	}
}

func TestLayer_DoesDatasetExist(t *testing.T) {
	tm := []*conf.TableMapping{
		{
			TableName: "Table1",
		},
		{
			TableName: "Table2",
		},
	}
	layer := Layer{
		cmgr: &conf.ConfigurationManager{
			Datalayer: &conf.Datalayer{
				TableMappings: tm,
			},
		},
	}
	exists := layer.DoesDatasetExist("Table1")
	if !exists {
		t.Errorf("Table1 does not exists")
	}

	exists = layer.DoesDatasetExist("SomeTable")
	if exists {
		t.Errorf("SomeTable should not exist")
	}
}

func TestLayer_GetContext(t *testing.T) {
	tm := []*conf.TableMapping{
		{
			TableName: "Table1",
		},
		{
			TableName: "Table2",
		},
	}
	layer := Layer{
		cmgr: &conf.ConfigurationManager{
			Datalayer: &conf.Datalayer{
				TableMappings: tm,
				BaseNameSpace: "http://data.test.io/adventureworks/test/",
			},
		},
	}

	context := layer.GetContext("Table1")
	ctx := make(map[string]interface{})
	namespaces := make(map[string]string)
	namespaces["ns0"] = "http://data.test.io/adventureworks/test/Table1/"
	namespaces["rdf"] = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
	ctx["namespaces"] = namespaces
	ctx["id"] = "@context"

	if !reflect.DeepEqual(context, ctx) {
		t.Errorf("%s != %s", context, ctx)
	}

}
