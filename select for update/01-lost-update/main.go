package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
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

// списывает amount, вычисляя новый баланс в памяти приложения.
func spend(ctx context.Context, pool *pgxpool.Pool, amount int, lock bool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	query := "SELECT balance FROM accounts WHERE id = 1"
	if lock {
		// FOR UPDATE захватывает эксклюзивную блокировку строки до конца транзакции.
		query += " FOR UPDATE"
	}
	var balance int
	if err := tx.QueryRow(ctx, query).Scan(&balance); err != nil {
		return err
	}

	time.Sleep(100 * time.Millisecond) // «вычисления» поверх прочитанного значения

	if _, err := tx.Exec(ctx, "UPDATE accounts SET balance = $1 WHERE id = 1", balance-amount); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// списывает amount одним оператором. UPDATE сам перечитывает строку
// после ожидания чужой блокировки, поэтому явная транзакция с FOR UPDATE здесь не нужна
func spendAtomic(ctx context.Context, pool *pgxpool.Pool, amount int) error {
	tag, err := pool.Exec(ctx,
		"UPDATE accounts SET balance = balance - $1 WHERE id = 1 AND balance >= $1", amount)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("недостаточно средств")
	}
	return nil
}

func balance(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	var b int
	if err := pool.QueryRow(ctx, "SELECT balance FROM accounts WHERE id = 1").Scan(&b); err != nil {
		return 0, fmt.Errorf("чтение баланса: %w", err)
	}
	return b, nil
}

// запускает два конкурентных списания и возвращает первую ошибку.
func spendTwice(ctx context.Context, pool *pgxpool.Pool, spendFn func(context.Context, *pgxpool.Pool, int) error) error {
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- spendFn(ctx, pool, 10)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func run(ctx context.Context, pool *pgxpool.Pool) error {
	if err := reset(ctx, pool); err != nil {
		return err
	}
	if err := spendTwice(ctx, pool, func(ctx context.Context, pool *pgxpool.Pool, amount int) error {
		return spend(ctx, pool, amount, false)
	}); err != nil {
		return err
	}
	b, err := balance(ctx, pool)
	if err != nil {
		return err
	}
	fmt.Printf("без FOR UPDATE: итог %d, одно списание потеряно\n", b)

	// Пример с FOR UPDATE
	if err := reset(ctx, pool); err != nil {
		return err
	}
	if err := spendTwice(ctx, pool, func(ctx context.Context, pool *pgxpool.Pool, amount int) error {
		return spend(ctx, pool, amount, true)
	}); err != nil {
		return err
	}
	b, err = balance(ctx, pool)
	if err != nil {
		return err
	}
	fmt.Printf("с FOR UPDATE: итог %d, оба списания учтены\n", b)

	// Блокировка нужна не всегда: один атомарный UPDATE с условием даёт
	// тот же результат без транзакции.
	if err := reset(ctx, pool); err != nil {
		return err
	}
	if err := spendTwice(ctx, pool, spendAtomic); err != nil {
		return err
	}
	b, err = balance(ctx, pool)
	if err != nil {
		return err
	}
	fmt.Printf("атомарный UPDATE без блокировки: итог %d\n", b)
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
