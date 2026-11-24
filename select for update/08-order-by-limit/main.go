package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultDatabaseURL = "postgres://postgres:postgres@localhost:55432/sfu?sslmode=disable"

type job struct {
	name     string
	priority int
}

type result struct {
	jobs []job
	err  error
}

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

// возвращает топ-2 по приоритету без блокировок: эталон для сравнения.
func snapshotTop2(ctx context.Context, pool *pgxpool.Pool) ([]job, error) {
	rows, err := pool.Query(ctx, "SELECT name, priority FROM jobs ORDER BY priority DESC LIMIT 2")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var got []job
	for rows.Next() {
		var j job
		if err := rows.Scan(&j.name, &j.priority); err != nil {
			return nil, err
		}
		got = append(got, j)
	}
	return got, nil
}

// запрашивает топ-2 с блокировкой строк. При обходе по
// индексу первой в выборке оказывается C, и вызов встаёт в очередь к
// держателю блокировки ещё до LIMIT; после разблокировки дочитывает
// остальные строки с их новыми значениями.
func pickTopPriorityJobs(ctx context.Context, pool *pgxpool.Pool) ([]job, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// Фиксируем план по индексу, иначе Seq Scan + Sort скроет эффект.
	if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT name, priority FROM jobs
		ORDER BY priority DESC
		LIMIT 2
		FOR UPDATE
	`)
	if err != nil {
		return nil, err
	}
	var got []job
	for rows.Next() {
		var j job
		if err := rows.Scan(&j.name, &j.priority); err != nil {
			rows.Close()
			return nil, err
		}
		got = append(got, j)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return got, nil
}

// воспроизводит гонку: T1 держит блокировку на C, T2 ждёт её
// в запросе топ-2, пока T1 меняет ключ сортировки C.
func observeTop2(ctx context.Context, pool *pgxpool.Pool) error {
	// T1 надолго блокирует C, самую приоритетную строку.
	tx1, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx1.Rollback(ctx)
	if _, err := tx1.Exec(ctx, "SELECT name FROM jobs WHERE name = 'C' FOR UPDATE"); err != nil {
		return err
	}

	// T2 запрашивает топ-2 и встаёт в очередь за C.
	t2done := make(chan result, 1)
	go func() {
		got, err := pickTopPriorityJobs(ctx, pool)
		t2done <- result{jobs: got, err: err}
	}()

	time.Sleep(150 * time.Millisecond) // даём T2 встать в очередь за C

	// Пока T2 ждёт, T1 меняет ключ сортировки C и коммитит.
	if _, err := tx1.Exec(ctx, "UPDATE jobs SET priority = 0 WHERE name = 'C'"); err != nil {
		return err
	}
	if err := tx1.Commit(ctx); err != nil {
		return err
	}

	r := <-t2done
	if r.err != nil {
		return fmt.Errorf("T2: %w", r.err)
	}
	fmt.Printf("T2 получила: %v\n", r.jobs)
	return nil
}

func run(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS jobs;
		CREATE TABLE jobs (name text PRIMARY KEY, priority int NOT NULL);
		INSERT INTO jobs VALUES ('A', 1), ('B', 2), ('C', 3);
		CREATE INDEX jobs_priority_idx ON jobs (priority DESC);
	`); err != nil {
		return fmt.Errorf("подготовка таблицы: %w", err)
	}

	if err := observeTop2(ctx, pool); err != nil {
		return err
	}
	top, err := snapshotTop2(ctx, pool)
	if err != nil {
		return err
	}
	fmt.Printf("актуальный топ-2 по свежему снимку: %v\n", top)
	fmt.Println("результат отличается от актуального топ-2, а порядок строк не соответствует ORDER BY")

	// Workaround из документации: FOR UPDATE во вложенном запросе, сортировка снаружи.
	// Порядок корректный, но так блокируются все строки таблицы, а не две.
	rows, err := pool.Query(ctx, `
		SELECT name, priority FROM (
			SELECT name, priority FROM jobs FOR UPDATE
		) ss
		ORDER BY priority DESC
		LIMIT 2
	`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var fixed []job
	for rows.Next() {
		var j job
		if err := rows.Scan(&j.name, &j.priority); err != nil {
			return err
		}
		fixed = append(fixed, j)
	}
	fmt.Printf("workaround с подзапросом: %v\n", fixed)
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
