package main

import (
	"database/sql"
	"fmt"
)

const CreateVersionTableStatement = `
CREATE TABLE [dbo].[%s](
	[TVer_FileName] [varchar](100) UNIQUE NOT NULL,
	[TVer_Date] [datetime] NOT NULL,
	[TVer_DbName] [varchar](50) NULL,
	[TVer_PK_ID] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
 CONSTRAINT [TVer_PK_ID] PRIMARY KEY CLUSTERED
(
	[TVer_PK_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
`

const CheckVersionTableColumnsQuery = `
DECLARE @ColCount int
DECLARE @ColExpected int

SET @ColExpected = 3

SELECT @ColCount = COUNT(*) FROM sys.columns
WHERE
    object_id = OBJECT_ID(N'[%[1]s]') AND
    name IN (N'TVer_FileName', N'TVer_Date', N'TVer_DbName')

IF @ColCount != @ColExpected
	raiserror (N'Missing columns in "%[1]s" version table: expected %%d columns, got %%d.',
		11, /* this is the "severity", it needs to be > 10 to trigger an error when we db.Exec() */
		1,  /* this is the "state", dunno */
		@ColExpected,
		@ColCount)
`

const InsertNewVersionStatement = `INSERT INTO [dbo].[%s] (TVer_FileName,TVer_Date,TVer_DbName) VALUES ('%s',GETDATE(),(SELECT db_name()))`

func CreateVersionTable(conn *sql.DB, tableName string) error {
	_, err := conn.Exec(fmt.Sprintf(CreateVersionTableStatement, tableName))
	return err
}

func CheckVersionTable(conn *sql.DB, tableName string) error {
	_, err := conn.Exec(fmt.Sprintf(CheckVersionTableColumnsQuery, tableName))
	return err
}

func InsertNewVersion(conn *sql.DB, tableName string, scriptName string) error {
	_, err := conn.Exec(fmt.Sprintf(InsertNewVersionStatement, tableName, scriptName))
	return err
}
