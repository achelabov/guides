// Долгая транзакция с блокировкой: внешний HTTP-вызов удерживает строку, конкуренты ждут.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultDatabaseURL = "postgres://postgres:postgres@localhost:55432/sfu?sslmode=disable"

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

func reset(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS accounts;
		CREATE TABLE accounts (id int PRIMARY KEY, balance int NOT NULL);
		INSERT INTO accounts VALUES (1, 100);
	`); err != nil {
		return fmt.Errorf("подготовка таблицы: %w", err)
	}
	return nil
}

type waitResult struct {
	d   time.Duration
	err error
}

// waiter имитирует конкурентное списание и измеряет, сколько оно ждало блокировку.
func waiter(ctx context.Context, pool *pgxpool.Pool) chan waitResult {
	waited := make(chan waitResult, 1)
	go func() {
		start := time.Now()
		// Обычный UPDATE ждёт чужую блокировку строки так же, как FOR UPDATE.
		_, err := pool.Exec(ctx, "UPDATE accounts SET balance = balance - 1 WHERE id = 1")
		waited <- waitResult{d: time.Since(start), err: err}
	}()
	return waited
}

// holdLockDuringHTTP анти-паттерн: блокировка взята до сетевого похода
// и держится всё время ожидания ответа.
func holdLockDuringHTTP(ctx context.Context, pool *pgxpool.Pool, gateway string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, "SELECT id FROM accounts WHERE id = 1 FOR UPDATE"); err != nil {
		return err
	}
	waited := waiter(ctx, pool)
	time.Sleep(100 * time.Millisecond) // конкурент встал в очередь за блокировкой
	if _, err := http.Get(gateway); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	r := <-waited
	if r.err != nil {
		return fmt.Errorf("конкурент: %w", r.err)
	}
	fmt.Printf("анти-паттерн: конкурент ждал %s (время HTTP-вызова)\n", r.d.Round(time.Millisecond))
	return nil
}

// httpBeforeTx правильный порядок: сначала внешний мир, потом короткая транзакция.
func httpBeforeTx(ctx context.Context, pool *pgxpool.Pool, gateway string) error {
	if _, err := http.Get(gateway); err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, "SELECT id FROM accounts WHERE id = 1 FOR UPDATE"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "UPDATE accounts SET balance = balance - 1 WHERE id = 1"); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	r := <-waiter(ctx, pool)
	if r.err != nil {
		return fmt.Errorf("конкурент: %w", r.err)
	}
	fmt.Printf("вызов до BEGIN: конкурент ждал %s\n", r.d.Round(time.Millisecond))
	return nil
}

func run(ctx context.Context, pool *pgxpool.Pool) error {
	if err := reset(ctx, pool); err != nil {
		return err
	}

	// «Внешний сервис», отвечающий 400 мс.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(400 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if err := holdLockDuringHTTP(ctx, pool, srv.URL); err != nil {
		return err
	}
	return httpBeforeTx(ctx, pool, srv.URL)
}

func main() {
	ctx := context.Background()
	pool := connect(ctx)
	defer pool.Close()

	if err := run(ctx, pool); err != nil {
		log.Fatal(err)
	}
}
