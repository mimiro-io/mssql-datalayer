package db

import (
	"encoding/base64"
	"fmt"
	goblin "github.com/franela/goblin"
	"github.com/mimiro-io/mssqldatalayer/internal/conf"
	. "github.com/onsi/gomega"
	"strings"
	"testing"
	"time"
)

func TestNewQueryWithFullQuery(t *testing.T) {
	g := goblin.Goblin(t)
	RegisterFailHandler(func(m string, _ ...int) { g.Fail(m) })

	g.Describe("when building not configuring CDC", func() {
		g.It("should return a full query", func() {
			tm := []*conf.TableMapping{
				{
					TableName: "Table1",
				},
			}

			layer := &conf.Datalayer{
				Schema:        "dbo",
				TableMappings: tm,
			}
			query := NewQuery(DatasetRequest{}, layer.TableMappings[0], layer)
			Expect(query).Should(BeAssignableToTypeOf(FullQuery{}))

			q := query.BuildQuery()
			g.Assert(q).Equal("SELECT  * FROM [dbo].[Table1]")
		})

		g.It("should be full query if not since is provided", func() {
			tm := []*conf.TableMapping{
				{
					TableName:  "Table1",
					CDCEnabled: true,
				},
			}

			layer := &conf.Datalayer{
				Schema:        "dbo",
				TableMappings: tm,
			}

			query := NewQuery(DatasetRequest{}, layer.TableMappings[0], layer)
			Expect(query).Should(BeAssignableToTypeOf(FullQuery{}))

			q := query.BuildQuery()
			g.Assert(q).Equal("SELECT  * FROM [dbo].[Table1]")
		})
	})

}

func TestNewQuery_WithCDC(t *testing.T) {
	g := goblin.Goblin(t)
	RegisterFailHandler(func(m string, _ ...int) { g.Fail(m) })

	g.Describe("when set up for cdc", func() {
		g.It("should return a cdc query", func() {
			tm := []*conf.TableMapping{
				{
					TableName:  "Table1",
					CDCEnabled: true,
				},
			}

			layer := &conf.Datalayer{
				TableMappings: tm,
			}

			dt := time.Now()
			s := fmt.Sprintf("%s", dt.Format(time.RFC3339))

			token := base64.StdEncoding.EncodeToString([]byte(s))

			query := NewQuery(DatasetRequest{Since: token}, layer.TableMappings[0], layer)
			Expect(query).Should(BeAssignableToTypeOf(CDCQuery{}))

			g.Assert(strings.Contains(query.BuildQuery(), "SELECT t.* FROM [cdc].[dbo_Table1_CT]")).IsTrue()
		})
	})

}
