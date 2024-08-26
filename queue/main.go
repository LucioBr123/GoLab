package main

// https://github.com/LucioBr123/GoLab/blob/master/queue/main.go
// Exemplo de  processamento em fila garantindo que não faz a mesma tarefa duas vezes
// mesmo com concorrencia (garantido pelo Mutex)

import (
	"fmt"
	"sync"
	"time"
)

type TableKey struct {
	Table string
	Key   int
}

type Queue struct {
	Objects []TableKey
	mu      sync.Mutex //to sync access
}

type DataBase struct {
	Objects []TableKey
	mu      sync.Mutex
}

func (q *Queue) Push(t TableKey) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.Objects = append(q.Objects, t)
}

func (db *DataBase) Push(t TableKey) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.Objects = append(db.Objects, t)
}

func (db *DataBase) Exists(t TableKey) bool {
	for _, obj := range db.Objects {
		if obj.Table == t.Table && obj.Key == t.Key {
			fmt.Println("Cancel insert: Object exists")
			return true
		}
	}
	return false
}

func (q *Queue) Process(db *DataBase) string {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.Objects) == 0 {
		return "queue is empty"
	}

	item := q.Objects[0]

	q.Objects = q.Objects[1:]

	if !db.Exists(item) {
		db.Push(item)
	}

	return ""
}

func main() {
	queue := &Queue{}
	db := &DataBase{}

	// Push items to queue
	for i := 0; i < 50; i++ {
		queue.Push(TableKey{Table: "table 1", Key: i})
	}
	for i := 0; i < 50; i++ {
		queue.Push(TableKey{Table: "table 2", Key: i})
	}

	fmt.Println(queue.Objects)

	// "api while processing"
	go func() {
		fmt.Print("processing...")
		for {
			if err := queue.Process(db); err != "" {
				fmt.Println(err)
				fmt.Println("Processados", len(db.Objects))
				time.Sleep(10 * time.Second)
			}
		}
	}()

	go func() {
		// Push items to queue
		for {
			fmt.Print("pushing repeating ...")
			time.Sleep(time.Second * 10)
			for i := 0; i < 10; i++ {
				queue.Push(TableKey{Table: "table 1", Key: i})
			}
			for i := 0; i < 10; i++ {
				queue.Push(TableKey{Table: "table 2", Key: i})
			}

			// Push items to queue
			for i := 0; i < 10; i++ {
				queue.Push(TableKey{Table: "table 1", Key: i})
			}
			for i := 0; i < 10; i++ {
				queue.Push(TableKey{Table: "table 2", Key: i})
			}
		}
	}()

	time.Sleep(time.Second * 100) // no stop
}
