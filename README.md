### migrate

Simple tool for automatically installing missing database versions.
  
It will check for migrations from provided folder that are not present in
versions table from database, and attempt to install them.

### installation

via go get: `go get github.com/NaturalSolutions/migrate`  
or from [release page](https://github.com/NaturalSolutions/migrate/releases)
 
### usage

```bash
$ migrate.exe -h
Usage of C:\Users\NaturalUser\home\go\bin\migrate.exe:
  -TVersion string
        Version table name (default "TVersion")
  -continueOnError
        Continue on error (only with -noPrompt)
  -database string
        Db name
  -folder string
        Migrations folder (default ".")
  -noPrompt
        Disable prompt
  -pass string
        Db pass
  -port int
        Db port (default 1433)
  -print
        Do not apply missing migrations, print script names only
  -scheme string
        DBMS scheme (default "mssql")
  -server string
        Db instance (default ".\\nsbdd")
  -startAt int
        Only apply migrations that have number >= to this value
  -stopAt int
        Only apply migrations that have number < to this value
  -user string
        Db user (default "nsapp")
  -v    verbose
  -version
        Print version & exit
```
