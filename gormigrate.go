package gomigrate

import (
	"database/sql"
	"errors"
	"fmt"
)

const (
	initSchemaMigrationID = "SCHEMA_INIT"
)

// MigrateFunc is the func signature for migrating.
type MigrateFunc func(*sql.Tx) error

// RollbackFunc is the func signature for rollbacking.
type RollbackFunc func(*sql.Tx) error

// InitSchemaFunc is the func signature for initializing the schema.
type InitSchemaFunc func(*sql.Tx) error

// Migration represents a database migration (a modification to be made on the database).
type Migration struct {
	// ID is the migration identifier. Usually a timestamp like "201601021504".
	ID string
	// Migrate is a function that will br executed while running this migration.
	Migrate MigrateFunc
	// Rollback will be executed on rollback. Can be nil.
	Rollback RollbackFunc
}

// gomigrate represents a collection of all migrations of a database schema.
type gomigrate struct {
	db         *sql.DB
	tx         *sql.Tx
	migrations []*Migration
	initSchema InitSchemaFunc
}

// ReservedIDError is returned when a migration is using a reserved ID
type ReservedIDError struct {
	ID string
}

func (e *ReservedIDError) Error() string {
	return fmt.Sprintf(`gomigrate: Reserved migration ID: "%s"`, e.ID)
}

// DuplicatedIDError is returned when more than one migration have the same ID
type DuplicatedIDError struct {
	ID string
}

func (e *DuplicatedIDError) Error() string {
	return fmt.Sprintf(`gomigrate: Duplicated migration ID: "%s"`, e.ID)
}

var (

	// ErrRollbackImpossible is returned when trying to rollback a migration
	// that has no rollback function.
	ErrRollbackImpossible = errors.New("go-migrate: It's impossible to rollback this migration")

	// ErrNoMigrationDefined is returned when no migration is defined.
	ErrNoMigrationDefined = errors.New("go-migrate: No migration defined")

	// ErrMissingID is returned when the ID od migration is equal to ""
	ErrMissingID = errors.New("go-migrate: Missing ID in migration")

	// ErrNoRunMigration is returned when any run migration was found while
	// running RollbackLast
	ErrNoRunMigration = errors.New("go-migrate: Could not find last run migration")

	// ErrMigrationIDDoesNotExist is returned when migrating or rolling back to a migration ID that
	// does not exist in the list of migrations
	ErrMigrationIDDoesNotExist = errors.New("go-migrate: Tried to migrate to an ID that doesn't exist")

	// ErrUnknownPastMigration is returned if a migration exists in the DB that doesn't exist in the code
	ErrUnknownPastMigration = errors.New("go-migrate: Found migration in DB that does not exist in code")
)

// New returns a new gomigrate.
func New(db *sql.DB, migrations []*Migration) *gomigrate {
	return &gomigrate{
		db:         db,
		migrations: migrations,
	}
}

// InitSchema sets a function that is run if no migration is found.
// The idea is preventing to run all migrations when a new clean database
// is being migrating. In this function you should create all tables and
// foreign key necessary to your application.
func (g *gomigrate) InitSchema(initSchema InitSchemaFunc) {
	g.initSchema = initSchema
}

// Migrate executes all migrations that did not run yet.
func (g *gomigrate) Migrate() error {
	if !g.hasMigrations() {
		return ErrNoMigrationDefined
	}
	var targetMigrationID string
	if len(g.migrations) > 0 {
		mig := g.migrations[len(g.migrations)-1]
		targetMigrationID = mig.ID
	}
	return g.migrate(targetMigrationID)
}

// MigrateTo executes all migrations that did not run yet up to the migration that matches `migrationID`.
func (g *gomigrate) MigrateTo(migrationID string) error {
	if err := g.checkIDExist(migrationID); err != nil {
		return err
	}
	return g.migrate(migrationID)
}

func (g *gomigrate) migrate(migrationID string) error {
	if !g.hasMigrations() {
		return ErrNoMigrationDefined
	}

	if err := g.checkReservedID(); err != nil {
		return err
	}

	if err := g.checkDuplicatedID(); err != nil {
		return err
	}

	g.begin()
	defer g.rollback()

	if err := g.createMigrationTableIfNotExists(); err != nil {
		return err
	}

	if g.initSchema != nil {
		canInitializeSchema, err := g.canInitializeSchema()
		if err != nil {
			return err
		}
		if canInitializeSchema {
			if err := g.runInitSchema(); err != nil {
				return err
			}
			return g.commit()
		}
	}

	for _, migration := range g.migrations {
		if err := g.runMigration(migration); err != nil {
			return err
		}
		if migrationID != "" && migration.ID == migrationID {
			break
		}
	}
	return g.commit()
}

// There are migrations to apply if either there's a defined
// initSchema function or if the list of migrations is not empty.
func (g *gomigrate) hasMigrations() bool {
	return g.initSchema != nil || len(g.migrations) > 0
}

// Check whether any migration is using a reserved ID.
// For now there's only have one reserved ID, but there may be more in the future.
func (g *gomigrate) checkReservedID() error {
	for _, m := range g.migrations {
		if m.ID == initSchemaMigrationID {
			return &ReservedIDError{ID: m.ID}
		}
	}
	return nil
}

func (g *gomigrate) checkDuplicatedID() error {
	lookup := make(map[string]struct{}, len(g.migrations))
	for _, m := range g.migrations {
		if _, ok := lookup[m.ID]; ok {
			return &DuplicatedIDError{ID: m.ID}
		}
		lookup[m.ID] = struct{}{}
	}
	return nil
}

func (g *gomigrate) checkIDExist(migrationID string) error {
	for _, migrate := range g.migrations {
		if migrate.ID == migrationID {
			return nil
		}
	}
	return ErrMigrationIDDoesNotExist
}

// RollbackLast undo the last migration
func (g *gomigrate) RollbackLast() error {
	if len(g.migrations) == 0 {
		return ErrNoMigrationDefined
	}

	g.begin()
	defer g.rollback()

	lastRunMigration, err := g.getLastRunMigration()
	if err != nil {
		return err
	}

	if err := g.rollbackMigration(lastRunMigration); err != nil {
		return err
	}
	return g.commit()
}

// RollbackTo undoes migrations up to the given migration that matches the `migrationID`.
// Migration with the matching `migrationID` is not rolled back.
func (g *gomigrate) RollbackTo(migrationID string) error {
	if len(g.migrations) == 0 {
		return ErrNoMigrationDefined
	}

	if err := g.checkIDExist(migrationID); err != nil {
		return err
	}

	g.begin()
	defer g.rollback()

	for i := len(g.migrations) - 1; i >= 0; i-- {
		migration := g.migrations[i]
		if migration.ID == migrationID {
			break
		}
		migrationRan, err := g.migrationRan(migration)
		if err != nil {
			return err
		}
		if migrationRan {
			if err := g.rollbackMigration(migration); err != nil {
				return err
			}
		}
	}
	return g.commit()
}

func (g *gomigrate) getLastRunMigration() (*Migration, error) {
	for i := len(g.migrations) - 1; i >= 0; i-- {
		migration := g.migrations[i]

		migrationRan, err := g.migrationRan(migration)
		if err != nil {
			return nil, err
		}

		if migrationRan {
			return migration, nil
		}
	}
	return nil, ErrNoRunMigration
}

// RollbackMigration undo a migration.
func (g *gomigrate) RollbackMigration(m *Migration) error {
	g.begin()
	defer g.rollback()

	if err := g.rollbackMigration(m); err != nil {
		return err
	}
	return g.commit()
}

func (g *gomigrate) rollbackMigration(m *Migration) error {
	if m.Rollback == nil {
		return ErrRollbackImpossible
	}

	if err := m.Rollback(g.tx); err != nil {
		return err
	}

	sql := "DELETE FROM migrations WHERE id = ?"
	_, err := g.db.Exec(sql, m.ID)
	return err
}

func (g *gomigrate) runInitSchema() error {
	if err := g.initSchema(g.tx); err != nil {
		return err
	}
	if err := g.insertMigration(initSchemaMigrationID); err != nil {
		return err
	}

	for _, migration := range g.migrations {
		if err := g.insertMigration(migration.ID); err != nil {
			return err
		}
	}

	return nil
}

func (g *gomigrate) runMigration(migration *Migration) error {
	if len(migration.ID) == 0 {
		return ErrMissingID
	}

	migrationRan, err := g.migrationRan(migration)
	if err != nil {
		return err
	}
	if !migrationRan {
		if err := migration.Migrate(g.tx); err != nil {
			return err
		}

		if err := g.insertMigration(migration.ID); err != nil {
			return err
		}
	}
	return nil
}

func (g *gomigrate) createMigrationTableIfNotExists() error {
	// check if migration table exists in postgres
	sqlStr := "CREATE TABLE IF NOT EXISTS migrations (id VARCHAR(255) PRIMARY KEY, applied TIMESTAMP)"
	_, err := g.db.Exec(sqlStr)
	fmt.Println(err)
	return err
}

func (g *gomigrate) migrationRan(m *Migration) (bool, error) {
	var count int
	sqlstr := "SELECT COUNT(*) FROM migrations WHERE id = $1"
	row := g.db.QueryRow(sqlstr, m.ID)

	err := row.Scan(&count)
	if err != nil {
		return false, err
	}
	fmt.Println("count: ", count)
	return count > 0, err
}

// The schema can be initialised only if it hasn't been initialised yet
// and no other migration has been applied already.
func (g *gomigrate) canInitializeSchema() (bool, error) {
	migrationRan, err := g.migrationRan(&Migration{ID: initSchemaMigrationID})
	if err != nil {
		return false, err
	}
	if migrationRan {
		return false, nil
	}

	// If the ID doesn't exist, we also want the list of migrations to be empty
	var count int64
	err = g.db.QueryRow("SELECT COUNT(*) FROM migrations").Scan(&count)
	fmt.Println(err)
	return count == 0, err
}

func (g *gomigrate) unknownMigrationsHaveHappened() (bool, error) {
	sql := "SELECT id FROM migrations"
	rows, err := g.db.Query(sql)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	validIDSet := make(map[string]struct{}, len(g.migrations)+1)
	validIDSet[initSchemaMigrationID] = struct{}{}
	for _, migration := range g.migrations {
		validIDSet[migration.ID] = struct{}{}
	}

	for rows.Next() {
		var pastMigrationID string
		if err := rows.Scan(&pastMigrationID); err != nil {
			return false, err
		}
		if _, ok := validIDSet[pastMigrationID]; !ok {
			return true, nil
		}
	}

	return false, nil
}

func (g *gomigrate) insertMigration(id string) error {
	sql := fmt.Sprintf("INSERT INTO migrations (id, applied) VALUES ($1, NOW())")
	_, err := g.db.Exec(sql, id)
	return err
}

func (g *gomigrate) begin() {
	g.tx, _ = g.db.Begin()
}

func (g *gomigrate) commit() error {
	return g.tx.Commit()
}

func (g *gomigrate) rollback() {

	g.tx.Rollback()
}
