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

const recentWindow = "now()::timestamp - interval '60 seconds'"

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
		DROP TABLE IF EXISTS payments;
		CREATE TABLE payments (
			id serial PRIMARY KEY,
			user_id int NOT NULL,
			created_at timestamp NOT NULL DEFAULT now()
		);
	`); err != nil {
		return fmt.Errorf("подготовка таблицы: %w", err)
	}
	return nil
}

func countRecent(ctx context.Context, pool *pgxpool.Pool, userID int) (int, error) {
	var n int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM payments WHERE user_id = $1 AND created_at > "+recentWindow, userID).Scan(&n); err != nil {
		return 0, fmt.Errorf("проверка: %w", err)
	}
	return n, nil
}

// сохраняет платёж, если за последние 60 секунд не было записей.
// Обе транзакции проходят проверку одновременно, дубликат гарантирован.
func payBroken(ctx context.Context, pool *pgxpool.Pool, userID int) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var n int
	if err := tx.QueryRow(ctx,
		"SELECT count(*) FROM payments WHERE user_id = $1 AND created_at > "+recentWindow, userID).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	time.Sleep(100 * time.Millisecond) // обе транзакции успевают пройти проверку
	if _, err := tx.Exec(ctx, "INSERT INTO payments (user_id) VALUES ($1)", userID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// вставляет платёж, полагаясь на уникальный индекс:
// дубликат отклонится сам, конфликт не страшен.
func payWithOnConflict(ctx context.Context, pool *pgxpool.Pool, userID int) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		INSERT INTO payments (user_id) VALUES ($1)
		ON CONFLICT DO NOTHING
	`, userID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// сериализует проверку и вставку по пользователю:
// вторая транзакция ждёт не строку, а сам факт проверки.
func payWithAdvisoryLock(ctx context.Context, pool *pgxpool.Pool, userID int) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", userID); err != nil {
		return err
	}
	var n int
	if err := tx.QueryRow(ctx,
		"SELECT count(*) FROM payments WHERE user_id = $1 AND created_at > "+recentWindow, userID).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, "INSERT INTO payments (user_id) VALUES ($1)", userID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// запускает pay дважды конкурентно и возвращает число платежей за окно.
func runTwice(ctx context.Context, pool *pgxpool.Pool, pay func(context.Context, *pgxpool.Pool, int) error) (int, error) {
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- pay(ctx, pool, 7)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return 0, err
		}
	}
	return countRecent(ctx, pool, 7)
}

func run(ctx context.Context, pool *pgxpool.Pool) error {
	// Сломанный вариант: обе транзакции видят count = 0 и обе вставляют.
	if err := reset(ctx, pool); err != nil {
		return err
	}
	n, err := runTwice(ctx, pool, payBroken)
	if err != nil {
		return err
	}
	fmt.Printf("проверка-потом-вставка без защиты: платежей за 60 с = %d\n", n)

	// Фикс 1: уникальный индекс + ON CONFLICT DO NOTHING.
	// Скользящее окно "60 секунд" индексом не выразить: предикат частичного
	// индекса обязан быть IMMUTABLE, а now() таковым не является (ошибка 42P17).
	// Поэтому индекс бьёт время на минутные корзины: второй платёж того же
	// пользователя в ту же минуту отклонится. Для точного правила смотрите
	// advisory lock ниже.
	if err := reset(ctx, pool); err != nil {
		return err
	}
	if _, err := pool.Exec(ctx, `
		CREATE UNIQUE INDEX payments_one_per_minute
		ON payments (user_id, date_trunc('minute', created_at))
	`); err != nil {
		return fmt.Errorf("индекс: %w", err)
	}
	n, err = runTwice(ctx, pool, payWithOnConflict)
	if err != nil {
		return err
	}
	fmt.Printf("уникальный индекс + ON CONFLICT: платежей за 60 с = %d\n", n)

	// Фикс 2: advisory lock сериализует проверку и вставку по пользователю.
	// Блокировка снимается автоматически в конце транзакции.
	if err := reset(ctx, pool); err != nil {
		return err
	}
	n, err = runTwice(ctx, pool, payWithAdvisoryLock)
	if err != nil {
		return err
	}
	fmt.Printf("pg_advisory_xact_lock: платежей за 60 с = %d\n", n)
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
