package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"github.com/denisenkom/go-mssqldb"
	_ "github.com/denisenkom/go-mssqldb"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const VERSION = "v0.1"

var (
	dbScheme      = flag.String("scheme", "mssql", "DBMS scheme")
	dbPort        = flag.Int("port", 1433, "Db port")
	dbServer      = flag.String("server", ".\\nsbdd", "Db instance")
	dbName        = flag.String("database", "", "Db name")
	dbUser        = flag.String("user", "nsapp", "Db user")
	dbPass        = flag.String("pass", "", "Db pass")
	initVersion   = flag.Bool("init", false, "Create versions table & exit")
	startAt       = flag.Int("startAt", 0, "Only apply migrations that have number >= to this value")
	stopAt        = flag.Int("stopAt", 0, "Only apply migrations that have number < to this value")
	noPrompt      = flag.Bool("noPrompt", false, "Disable prompt")
	continueOnErr = flag.Bool("continueOnError", false, "Continue on error (only with -noPrompt)")
	printOnly     = flag.Bool("print", false, "Do not apply missing migrations, print script names only")
	fake          = flag.Bool("fake", false, "Only check migration validity, no commit, no db change (only with -noPrompt)")
	noUpVersion   = flag.Bool("noUpVersion", false, "Do not insert installed script in TVersion table, do it manually or in script")
	migrationsDir = flag.String("folder", ".", "Migrations folder")
	versionTable  = flag.String("TVersion", "TVersion", "Version table name")
	verbose       = flag.Bool("v", false, "Verbose")
	version       = flag.Bool("version", false, "Print version & exit")
)

var (
	versionsQuery       = "SELECT TVer_FileName FROM %s"
	regexUseDatabase    = regexp.MustCompile(`^\s*(?i)use\s+([[:alnum:]])+\s*$`)
	regexScriptNumber   = regexp.MustCompile(`^([[:digit:]]+)_`)
	supportedScriptExts = map[string]bool{".sql": true, ".txt": true}
)

func loadMigrations(dir string, startId int, stopId int) (scripts []SqlScript, err error) {
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	scripts = []SqlScript{}

	for _, fi := range fis {
		fext := filepath.Ext(fi.Name())
		if !supportedScriptExts[fext] {
			if *verbose {
				log.Printf("skipping file with unsupported extension \"%s\"", fi.Name())
			}
			continue
		}

		fpath := filepath.Join(dir, fi.Name())
		buf, err := ioutil.ReadFile(fpath)
		if err != nil {
			log.Printf("readfile \"%s\": %s", fpath, err)
			continue
		}

		scriptName := strings.TrimSuffix(fi.Name(), fext)
		script := SqlScript{
			// left-trim zeroes from scriptName, file name is inconsistant with db version
			Name:    strings.TrimLeft(scriptName, "0"),
			Content: string(buf),
		}

		// extract script number from script name
		if regexScriptNumber.MatchString(script.Name) {
			nbString := string(regexScriptNumber.FindSubmatch([]byte(script.Name))[1])
			script.Number, err = strconv.Atoi(nbString)
			if err != nil {
				log.Printf("couldn't extract script number from \"%s\": %s", script.Name, err)
			}
		}

		// do we have a "USE" clause ?
		if regexUseDatabase.Match(buf) {
			script.Db = string(regexUseDatabase.FindSubmatch(buf)[1])
		}

		// only add between startId & stopId
		if script.Number >= startId && (stopId <= 0 || script.Number < stopId) {
			scripts = append(scripts, script)
		}
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
	if *version {
		name, _ := os.Executable()
		fmt.Printf("%s %s\n", path.Base(name), VERSION)
		os.Exit(0)
	}

	if len(*dbPass) == 0 {
		usage("-pass required")
	}
	if len(*dbName) == 0 {
		usage("-database required")
	}
	versionsQuery = fmt.Sprintf(versionsQuery, *versionTable)

	if *verbose {
		// insert file name & line number to log prefix
		log.SetFlags(log.LstdFlags | log.Lshortfile)
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
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatalf("error connecting to db: %s", err)
	}

	// create version table if -init flag
	if *initVersion {
		err = CreateVersionTable(db, *versionTable)
		if err != nil {
			log.Fatalf("error creating versions table: %s", err)
		}
		log.Printf("successfuly created \"%s\" version table", *versionTable)
		os.Exit(0)
	}

	// check that versions table exist and passes integrity check
	err = CheckVersionTable(db, *versionTable)
	if err != nil {
		log.Printf("error checking versions table: %s", err)
		log.Fatal("try -init flag?")
	}

	// fetch all versions from db
	i, dbMigrations, err := getDbVersions(db)
	if err != nil {
		log.Fatalf("error querying versions table: %s", err)
	}
	log.Printf("found %d installed migrations in db \"%s\"", i, *dbName)

	// load migration scripts from migrations folder
	sqlScripts, err := loadMigrations(*migrationsDir, *startAt, *stopAt)
	if err != nil {
		log.Fatalf("error loading migrations: %s", err)
	}

	errors := false

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

		if *printOnly {
			log.Printf("to be applied: \"%s\"", script.Name)
			continue
		}
		log.Printf("applying: \"%s\"", script.Name)

		rd := bufio.NewReader(os.Stdin)

	prompt:
		var b []byte
		if !*noPrompt {
			fmt.Printf("run script? (Y)es, (n)o, (q)uit, (d)isplay, (f)ake (no db change): ")
			b, _, err = rd.ReadLine()
			if err != nil {
				log.Fatal(err)
			}
		}

		if len(b) == 0 {
			b = []byte{'Y'}
		}

		var noCommit = *fake
		switch strings.ToUpper(string(b))[0] {
		case 'Y':
			break
		case 'N':
			continue
		case 'Q':
			os.Exit(0)
		case 'F':
			noCommit = true
		case 'D':
			fmt.Fprintln(os.Stderr, script.Content)
			fallthrough
		default:
			fmt.Println()
			goto prompt
		}

		_, err = script.Execute(db, noCommit)
		if err != nil {
			log.Println(err, "\n")
			if *continueOnErr {
				errors = true
				continue
			}

			fmt.Fprint(os.Stderr, "\ncontinue to next script? y/N: ")
			b, _, err := rd.ReadLine()
			if err != nil {
				log.Fatal(err)
			}

			if len(b) == 0 || (strings.ToUpper(string(b))[0] != 'Y') {
				os.Exit(1)
			}
		} else {
			if !*noUpVersion {
				err = script.InsertVersion(db, *versionTable)
				if err != nil {
					log.Printf("error inserting script into \"%s\" version table: %s", *versionTable, err)

					// try to guess what error was made
					if mssqlErr, ok := err.(mssql.Error); ok {
						// 2627 is unique constraint violation
						if mssqlErr.Number == 2627 {
							log.Println("use -noUpVersion flag?")
						}
					}

					os.Exit(1)
				}
			}
			log.Println("OK")
		}
	}

	if errors {
		os.Exit(1)
	}
}
