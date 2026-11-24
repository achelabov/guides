package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultDatabaseURL = "postgres://postgres:postgres@localhost:55432/sfu?sslmode=disable"

const workers = 3

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
		DROP TABLE IF EXISTS tasks;
		CREATE TABLE tasks (
			id serial PRIMARY KEY,
			payload text NOT NULL,
			status text NOT NULL DEFAULT 'pending'
		);
		INSERT INTO tasks (payload) VALUES ('a'), ('b'), ('c'), ('d'), ('e'), ('f');
	`); err != nil {
		return fmt.Errorf("подготовка таблицы: %w", err)
	}
	return nil
}

// забирает одну свободную задачу. SKIP LOCKED пропускает строки, уже
// заблокированные чужими воркерами, поэтому конкурентные claim не ждут друг
// друга. UPDATE от имени той же транзакции фиксирует факт захвата: у задачи
// меняется статус, и повторно её никто не возьмёт.
func claim(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var id int
	err = tx.QueryRow(ctx, `
		UPDATE tasks SET status = 'processing'
		WHERE id = (
			SELECT id FROM tasks
			WHERE status = 'pending'
			ORDER BY id
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id
	`).Scan(&id)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return id, nil
}

func run(ctx context.Context, pool *pgxpool.Pool) error {
	if err := reset(ctx, pool); err != nil {
		return err
	}

	var (
		mu      sync.Mutex
		claimed = map[int][]int{}
		errs    []error
		wg      sync.WaitGroup
	)
	for w := 1; w <= workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				id, err := claim(ctx, pool)
				if errors.Is(err, pgx.ErrNoRows) {
					return
				}
				if err != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("воркер %d: %w", w, err))
					mu.Unlock()
					return
				}
				time.Sleep(50 * time.Millisecond) // обработка задачи
				mu.Lock()
				claimed[w] = append(claimed[w], id)
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	seen := map[int]bool{}
	for w := 1; w <= workers; w++ {
		fmt.Printf("воркер %d: задачи %v\n", w, claimed[w])
		for _, id := range claimed[w] {
			if seen[id] {
				return fmt.Errorf("задача %d обработана дважды", id)
			}
			seen[id] = true
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("ошибки воркеров: %v", errs)
	}
	fmt.Printf("обработано задач: %d из 6, дублей нет\n", len(seen))
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
