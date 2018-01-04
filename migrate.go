package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	dbScheme      = flag.String("scheme", "mssql", "DBMS scheme")
	dbPort        = flag.Int("port", 1433, "Db port")
	dbServer      = flag.String("server", ".\\nsbdd", "Db instance")
	dbName        = flag.String("database", "", "Db name")
	dbUser        = flag.String("user", "nsapp", "Db user")
	dbPass        = flag.String("pass", "", "Db pass")
	migrationsDir = flag.String("folder", ".", "Migrations folder")
	versionTable  = flag.String("TVersion", "TVersion", "Version table name")
	verbose       = flag.Bool("v", false, "verbose")
)

var (
	versionsQuery       = fmt.Sprintf("SELECT TVer_FileName FROM %s", *versionTable)
	regexUseDatabase    = regexp.MustCompile(`^\s*(?i)use\s+([[:alnum:]])+\s*$`)
	supportedScriptExts = map[string]bool{".sql": true, ".txt": true}
)

type SqlScript struct {
	Db        string
	Name      string
	Content   string
	Installed bool
}

func loadMigrations(dir string) (scripts []SqlScript, err error) {
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	scripts = []SqlScript{}

	for _, fi := range fis {
		fext := filepath.Ext(fi.Name())
		if !supportedScriptExts[fext] {
			if *verbose {
				log.Printf("skipping file with unsupported extension: %s, %s", fi.Name(), fext)
			}
			continue
		}

		fpath := filepath.Join(dir, fi.Name())
		buf, err := ioutil.ReadFile(fpath)
		if err != nil {
			log.Printf("readfile %s: %s", fpath, err)
			continue
		}

		scriptName := strings.TrimSuffix(fi.Name(), fext)
		script := SqlScript{
			// left-trim zeroes from scriptName, file name is inconsistant with db version
			Name:    strings.TrimLeft(scriptName, "0"),
			Content: string(buf),
		}

		if regexUseDatabase.Match(buf) {
			matches := regexUseDatabase.FindSubmatch(buf)
			if matches != nil && len(matches) > 1 {
				script.Db = string(matches[1])
			}
		}

		scripts = append(scripts, script)
	}

	return scripts, nil
}

func getDbVersions(db *sql.DB) (int, map[string]bool, error) {
	if *verbose {
		log.Printf("getDbVersions: %s", versionsQuery)
	}

	rows, err := db.Query(versionsQuery)
	if err != nil {
		return 0, nil, err
	}

	var migrationName string
	var installedMigrations = map[string]bool{}
	i := 0
	for rows.Next() {
		err := rows.Scan(&migrationName)
		if err != nil {
			log.Printf("rows.Scan: %s", err)
			continue
		}

		if *verbose {
			log.Printf("already installed: %s", migrationName)
		}
		installedMigrations[migrationName] = true
		i++
	}
	return i, installedMigrations, nil
}

func usage(reason string) {
	flag.Usage()
	if len(reason) > 0 {
		fmt.Fprintln(os.Stderr, "\n", reason)
	}
	os.Exit(1)
}

func init() {
	flag.Parse()
	if len(*dbPass) == 0 {
		usage("-pass required")
	}
	if len(*dbName) == 0 {
		usage("-database required")
	}
}

func main() {
	dbConnString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;", *dbServer, *dbUser, *dbPass, *dbPort)
	if len(*dbName) > 0 {
		dbConnString += fmt.Sprintf("database=%s;", *dbName)
	}
	log.Println("using connString", dbConnString)

	db, err := sql.Open(*dbScheme, dbConnString)
	if err != nil {
		log.Fatalf("error opening db: %s", err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatalf("error connecting to db: %s", err)
	}

	// fetch all versions from db
	i, dbMigrations, err := getDbVersions(db)
	if err != nil {
		log.Fatalf("error querying versions table: %s", err)
	}
	log.Printf("found %d installed migrations in db \"%s\"", i, *dbName)

	// load migration scripts from migrations folder
	sqlScripts, err := loadMigrations(*migrationsDir)
	if err != nil {
		log.Fatalf("error loading migrations: %s", err)
	}

	for _, script := range sqlScripts {
		if dbMigrations[script.Name] {
			if *verbose {
				log.Printf("skipping \"%s\"", script.Name)
			}
			continue
		} else if len(script.Db) > 0 {
			log.Printf("skipping script with clause \"USE %s\" \"%s\" - apply it manually", script.Db, script.Name)
			continue
		}

		// exec migration
		log.Printf("applying \"%s\"", script.Name)
	}
}
