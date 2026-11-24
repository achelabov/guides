// NOWAIT и lock_timeout: fail-fast вместо бесконечного ожидания блокировки.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultDatabaseURL = "postgres://postgres:postgres@localhost:55432/sfu?sslmode=disable"

// errInvoiceBusy: NOWAIT не стал ждать блокировку строки.
var errInvoiceBusy = errors.New("счёт занят, попробуйте позже")

// errLockTimeout: ожидание блокировки превысило lock_timeout.
var errLockTimeout = errors.New("lock_timeout: PostgreSQL сам прервал ожидание")

func connect(ctx context.Context) *pgxpool.Pool {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = defaultDatabaseURL
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		log.Fatalf("подключение: %v", err)
	}
	return pool
}

func isLockUnavailable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55P03"
}

// holdInvoice надолго блокирует счёт-фактуру, имитируя зависшую транзакцию.
// Транзакцию возвращаем наружу: блокировка нужна держаться дольше функции.
func holdInvoice(ctx context.Context, pool *pgxpool.Pool) (pgx.Tx, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	var status string
	if err := tx.QueryRow(ctx, "SELECT status FROM invoices WHERE id = 1 FOR UPDATE").Scan(&status); err != nil {
		tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

// getInvoiceNowait не ждёт: если строку нельзя заблокировать сразу, будет ошибка.
func getInvoiceNowait(ctx context.Context, pool *pgxpool.Pool, id int) (string, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var status string
	err = tx.QueryRow(ctx, "SELECT status FROM invoices WHERE id = $1 FOR UPDATE NOWAIT", id).Scan(&status)
	if err != nil {
		if isLockUnavailable(err) {
			return "", errInvoiceBusy
		}
		return "", err
	}
	return status, tx.Commit(ctx)
}

// getInvoiceWithTimeout ждёт блокировку, но не дольше lock_timeout.
func getInvoiceWithTimeout(ctx context.Context, pool *pgxpool.Pool, id int) (string, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '300ms'"); err != nil {
		return "", err
	}
	var status string
	start := time.Now()
	err = tx.QueryRow(ctx, "SELECT status FROM invoices WHERE id = $1 FOR UPDATE", id).Scan(&status)
	if err != nil {
		if isLockUnavailable(err) {
			return "", fmt.Errorf("%w через %s с кодом 55P03", errLockTimeout,
				time.Since(start).Round(time.Millisecond))
		}
		return "", err
	}
	return status, tx.Commit(ctx)
}

func run(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS invoices;
		CREATE TABLE invoices (id int PRIMARY KEY, status text NOT NULL);
		INSERT INTO invoices VALUES (1, 'open');
	`); err != nil {
		return fmt.Errorf("подготовка таблицы: %w", err)
	}

	// Транзакция, которая надолго заняла строку.
	tx1, err := holdInvoice(ctx, pool)
	if err != nil {
		return err
	}
	defer tx1.Rollback(ctx)

	_, err = getInvoiceNowait(ctx, pool, 1)
	if errors.Is(err, errInvoiceBusy) {
		fmt.Println(err) // счёт-фактура занята, попробуйте позже
	} else if err != nil {
		return err
	}

	_, err = getInvoiceWithTimeout(ctx, pool, 1)
	if errors.Is(err, errLockTimeout) {
		fmt.Println(err) // lock_timeout: PostgreSQL сам прервал ожидание через 312ms с кодом 55P03
	} else if err != nil {
		return err
	}
	return nil
}

func main() {
	ctx := context.Background()
	pool := connect(ctx)
	defer pool.Close()

	if err := run(ctx, pool); err != nil {
		log.Fatal(err)
	}
}
