package postgresql

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/errwrap"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/lib/pq"
)

const (
	tableNameAttr        = "name"
	tableCreateTableAttr = "create_table"
	columnAttr           = "column"
	columnNameAttr       = "name"
	columnTypeAttr       = "type"
	columnMaxLengthAttr  = "max_length"
)

func resourcePostgreSQLTable() *schema.Resource {
	return &schema.Resource{
		Create: resourcePostgreSQLTableCreate,
		Read:   resourcePostgreSQLTableRead,
		Update: resourcePostgreSQLTableUpdate,
		Delete: resourcePostgreSQLTableDelete,
		Exists: resourcePostgreSQLTableExists,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			tableNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The name of the table",
			},
			columnAttr: {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						columnNameAttr: {
							Type:     schema.TypeString,
							Required: true,
						},
						columnTypeAttr: {
							Type:     schema.TypeString,
							Required: true,
						},
						columnMaxLengthAttr: {
							Type:     schema.TypeInt,
							Optional: true,
						},
					},
				},
			},
		},
	}
}

func resourcePostgreSQLTableCreate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	tableName := d.Get(tableNameAttr).(string)

	sql := fmt.Sprintf("CREATE TABLE %s ()", pq.QuoteIdentifier(tableName))
	log.Printf("[DEBUG] table create: `%s`", sql)
	if _, err := c.DB().Exec(sql); err != nil {
		return errwrap.Wrapf(fmt.Sprintf("Error creating table %s: {{err}}", tableName), err)
	}

	d.SetId(tableName)

	return resourcePostgreSQLTableUpdateImpl(d, meta)
}

func resourcePostgreSQLTableDelete(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	d.SetId("")

	return nil
}

const tableExistsQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' and table_name = $1"

func resourcePostgreSQLTableExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	c := meta.(*Client)
	c.catalogLock.RLock()
	defer c.catalogLock.RUnlock()

	log.Printf("[DEBUG] table exists: `%s`", d.Id())

	var tableName string
	err := c.DB().QueryRow(tableExistsQuery, d.Id()).Scan(&tableName)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	}

	return true, nil
}

func resourcePostgreSQLTableRead(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	c.catalogLock.RLock()
	defer c.catalogLock.RUnlock()

	return resourcePostgreSQLTableReadImpl(d, meta)
}

const columnsDescribeQuery = `
	SELECT 
		column_name as name, 
		column_default as default_expr,
		is_nullable,
		udt_name as column_type,
		character_maximum_length as max_length
	FROM information_schema.columns 
	WHERE table_name = $1
	ORDER BY ordinal_position
	`

var typeAliases = map[string]string{
	"int4": "int",
}

func useTypeAlias(columnType string) string {
	if v, found := typeAliases[columnType]; found {
		return v
	}
	return columnType
}

func orDefault(data sql.NullString, fallback string) string {
	if data.Valid {
		return data.String
	}
	return fallback
}

func parseIsNullable(data string) bool {
	return strings.ToLower(data) == "yes"
}

func columns(db *sql.DB, tableName string) ([]interface{}, error) {
	var columns []interface{}
	rows, _ := db.Query(columnsDescribeQuery, tableName)
	for rows.Next() {
		var name, columnType string
		var defaultExpr sql.NullString
		var maxLength sql.NullInt64
		var isNullable string

		err := rows.Scan(&name, &defaultExpr, &isNullable, &columnType, &maxLength)
		if err != nil {
			return columns, err
		}
		column := map[string]interface{}{
			columnNameAttr: name,
			columnTypeAttr: useTypeAlias(columnType),
		}
		if maxLength.Valid {
			column[columnMaxLengthAttr] = maxLength.Int64
		}
		columns = append(columns, column)
	}
	return columns, nil
}

const tableDescribeQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' and table_name = $1"

func resourcePostgreSQLTableReadImpl(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	db := c.DB()
	tableID := d.Id()

	var tableName string

	log.Printf("[DEBUG] table read: `%s`", tableID)

	err := db.QueryRow(tableDescribeQuery, tableID).Scan(
		&tableName,
	)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("[WARN] PostgreSQL TABLE (%s) not found", tableID)
		d.SetId("")
		return nil
	case err != nil:
		return errwrap.Wrapf(fmt.Sprintf("Error reading TABLE (%s): {{err}}", tableID), err)
	}

	d.Set(tableNameAttr, tableName)
	d.SetId(tableName)

	columns, err := columns(db, tableName)

	if err != nil {
		return errwrap.Wrapf(fmt.Sprintf("Error reading columns TABLE (%s): {{err}}", tableID), err)
	}

	if err := d.Set(columnAttr, columns); err != nil {
		return errwrap.Wrapf(fmt.Sprintf("Error setting columns TABLE (%s): {{err}}", tableID), err)
	}

	return nil
}

func renameTableIfNeeded(d *schema.ResourceData, db *sql.DB) error {
	if !d.HasChange(columnNameAttr) {
		return nil
	}

	oraw, nraw := d.GetChange(columnNameAttr)
	old := oraw.(string)
	new := nraw.(string)

	if new == "" {
		return errors.New("Error setting table name to an empty string")
	}

	sql := fmt.Sprintf("ALTER TABLE %s RENAME TO %s", pq.QuoteIdentifier(old), pq.QuoteIdentifier(new))
	log.Printf("[DEBUG] table rename: `%s`", sql)
	if _, err := db.Exec(sql); err != nil {
		return errwrap.Wrapf("Error updating table NAME: {{err}}", err)
	}

	d.SetId(new)

	return nil
}

func resourcePostgreSQLTableUpdate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	return resourcePostgreSQLTableUpdateImpl(d, meta)
}

func resourcePostgreSQLTableUpdateImpl(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	db := c.DB()

	if err := renameTableIfNeeded(d, db); err != nil {
		return err
	}

	return resourcePostgreSQLTableReadImpl(d, meta)
}
