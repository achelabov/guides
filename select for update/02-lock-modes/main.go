package main

import (
	"context"
	"errors"
	"fmt"
	"log"
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
		DROP TABLE IF EXISTS orders;
		DROP TABLE IF EXISTS users;
		CREATE TABLE users (id int PRIMARY KEY);
		CREATE TABLE orders (id serial PRIMARY KEY, user_id int NOT NULL REFERENCES users(id));
		INSERT INTO users VALUES (1);
	`); err != nil {
		return fmt.Errorf("подготовка таблиц: %w", err)
	}
	return nil
}

// блокирует пользователя в заданном режиме
// и вставляет заказ, если родительская строка позволяет.
func createOrderWhileUserLocked(ctx context.Context, pool *pgxpool.Pool, mode string) (time.Duration, error) {
	// Транзакция держит своё соединение из пула до Commit,
	// конкурентная вставка ниже возьмёт другой.
	tx1, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx1.Rollback(ctx)

	var id int
	if err := tx1.QueryRow(ctx, "SELECT id FROM users WHERE id = 1 FOR "+mode).Scan(&id); err != nil {
		return 0, err
	}

	// Вставка заказа из «другого сервиса» с ограничением по времени.
	// Вставка по FK берёт FOR KEY SHARE на строке users и конфликтует
	// с FOR UPDATE, но не с FOR NO KEY UPDATE.
	tryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	start := time.Now()
	_, err = pool.Exec(tryCtx, "INSERT INTO orders (user_id) VALUES (1)")
	cancel()
	if err != nil {
		return 0, err
	}
	if err := tx1.Commit(ctx); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

func run(ctx context.Context, pool *pgxpool.Pool) error {
	if err := reset(ctx, pool); err != nil {
		return err
	}

	// Обычный UPDATE берёт FOR NO KEY UPDATE неявно, пока вы не меняете колонки
	// уникального индекса. FOR UPDATE нужен, когда строку удаляют или меняют
	// ключ: только он блокирует чужой DELETE.
	for _, mode := range []string{"UPDATE", "NO KEY UPDATE"} {
		elapsed, err := createOrderWhileUserLocked(ctx, pool, mode)
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			fmt.Printf("FOR %-14s: вставка ждала 5c, отменена по контексту\n", mode)
		case err != nil:
			return err
		default:
			fmt.Printf("FOR %-14s: вставка прошла за %s\n", mode, elapsed.Round(time.Millisecond))
		}
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
