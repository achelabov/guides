package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
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
		INSERT INTO accounts VALUES (1, 100), (2, 100);
	`); err != nil {
		return fmt.Errorf("подготовка таблицы: %w", err)
	}
	return nil
}

func transfer(ctx context.Context, pool *pgxpool.Pool, fromID, toID int, ordered bool) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Блокировки берём в одном и том же порядке у всех транзакций, иначе
	// возможен deadlock. Направление перевода при этом сохраняем.
	lockFirst, lockSecond := fromID, toID
	if ordered && lockFirst > lockSecond {
		lockFirst, lockSecond = lockSecond, lockFirst
	}

	if _, err := tx.Exec(ctx, "SELECT id FROM accounts WHERE id = $1 FOR UPDATE", lockFirst); err != nil {
		return err
	}
	time.Sleep(80 * time.Millisecond) // между двумя блокировками проходит время
	if _, err := tx.Exec(ctx, "SELECT id FROM accounts WHERE id = $1 FOR UPDATE", lockSecond); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, "UPDATE accounts SET balance = balance - 10 WHERE id = $1", fromID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "UPDATE accounts SET balance = balance + 10 WHERE id = $1", toID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// перезапускает транзакцию после deadlock (40P01) и lock_timeout (55P03).
func withRetry(fn func() error) error {
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) ||
			(pgErr.Code != "40P01" && pgErr.Code != "55P03") || attempt >= 3 {
			return err
		}
		fmt.Printf("%s, повтор %d\n", pgErr.Code, attempt)
		time.Sleep(50 * time.Millisecond)
	}
}

func balances(ctx context.Context, pool *pgxpool.Pool) (int, int, error) {
	var b1, b2 int
	if err := pool.QueryRow(ctx, "SELECT balance FROM accounts WHERE id = 1").Scan(&b1); err != nil {
		return 0, 0, fmt.Errorf("баланс 1: %w", err)
	}
	if err := pool.QueryRow(ctx, "SELECT balance FROM accounts WHERE id = 2").Scan(&b2); err != nil {
		return 0, 0, fmt.Errorf("баланс 2: %w", err)
	}
	return b1, b2, nil
}

// запускает два встречных перевода и возвращает ошибки обеих транзакций.
func runPair(ctx context.Context, pool *pgxpool.Pool, ordered bool, retry bool) []error {
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, pair := range [][2]int{{1, 2}, {2, 1}} {
		wg.Add(1)
		go func(fromID, toID int) {
			defer wg.Done()
			transferFn := func() error {
				return transfer(ctx, pool, fromID, toID, ordered)
			}
			if retry {
				errs <- withRetry(transferFn)
			} else {
				errs <- transferFn()
			}
		}(pair[0], pair[1])
	}
	wg.Wait()
	close(errs)

	var result []error
	for err := range errs {
		result = append(result, err)
	}
	return result
}

func isDeadlock(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40P01"
}

func run(ctx context.Context, pool *pgxpool.Pool) error {
	// Сценарий 1: разный порядок блокировок, PostgreSQL убивает одну транзакцию.
	fmt.Println("=== 1. Блокировки в разном порядке ===")
	if err := reset(ctx, pool); err != nil {
		return err
	}
	errs := runPair(ctx, pool, false, false)
	for _, err := range errs {
		if isDeadlock(err) {
			fmt.Println("без сортировки: одна транзакция погибла с 40P01 (deadlock)")
		}
	}
	b1, b2, err := balances(ctx, pool)
	if err != nil {
		return err
	}
	fmt.Printf("балансы: %d и %d (сумма 200: погибшая транзакция откатилась, деньги не потерялись)\n", b1, b2)

	// Сценарий 2: то же, но с retry: deadlock случается, но повтор проходит.
	fmt.Println("=== 2. То же, но с retry ===")
	if err := reset(ctx, pool); err != nil {
		return err
	}
	errs = runPair(ctx, pool, false, true)
	failed := 0
	for _, err := range errs {
		if err != nil {
			failed++
		}
	}
	fmt.Printf("без порядка, с retry: неуспешных транзакций %d\n", failed)
	b1, b2, err = balances(ctx, pool)
	if err != nil {
		return err
	}
	fmt.Printf("балансы: %d и %d (оба перевода доехали)\n", b1, b2)

	// Сценарий 3: единый порядок блокировок, deadlock не возникает вовсе.
	fmt.Println("=== 3. Блокировки по возрастанию ID ===")
	if err := reset(ctx, pool); err != nil {
		return err
	}
	errs = runPair(ctx, pool, true, false)
	failed = 0
	for _, err := range errs {
		if err != nil {
			failed++
		}
	}
	fmt.Printf("с единым порядком ID: неуспешных транзакций %d\n", failed)
	b1, b2, err = balances(ctx, pool)
	if err != nil {
		return err
	}
	fmt.Printf("балансы: %d и %d\n", b1, b2)
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
