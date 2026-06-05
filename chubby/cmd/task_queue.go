package main

import (
	"cos518project/chubby/api"
	"cos518project/chubby/client"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Task struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	Status      string    `json:"status"`
	AssignedTo  string    `json:"assigned_to"`
	CreatedAt   time.Time `json:"created_at"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
}

var taskTypes = []string{
	"send_email", "generate_report", "process_payment",
	"resize_image", "send_notification", "backup_database",
	"sync_data", "calculate_stats",
}

const NUM_TASKS = 8
const TASK_LOCK_PREFIX = "TaskQueue/task_"
const QUEUE_LOCK = "TaskQueue/queue_lock"

func runProducer(workerID string) {
	log.Printf("[PRODUCER] Starting up, creating %d tasks...", NUM_TASKS)
	sess, err := client.InitSession(api.ClientID("producer_" + workerID))
	if err != nil {
		log.Fatalf("[PRODUCER] Cannot connect to Chubby cluster: %v", err)
	}
	log.Printf("[PRODUCER] Connected successfully!")
	sess.OpenLock(QUEUE_LOCK)
	for i := 1; i <= NUM_TASKS; i++ {
		taskID := fmt.Sprintf("task_%d", i)
		lockPath := api.FilePath(TASK_LOCK_PREFIX + taskID)
		task := Task{
			ID: taskID, Type: taskTypes[rand.Intn(len(taskTypes))],
			Status: "pending", CreatedAt: time.Now(),
		}
		taskJSON, _ := json.Marshal(task)
		sess.OpenLock(lockPath)
		ok, err := sess.TryAcquireLock(lockPath, api.EXCLUSIVE)
		if err != nil || !ok {
			log.Printf("[PRODUCER] Cannot acquire lock for %s", taskID)
			continue
		}
		sess.WriteContent(lockPath, string(taskJSON))
		sess.ReleaseLock(lockPath)
		log.Printf("[PRODUCER] Created task #%d | type: %-20s | status: pending", i, task.Type)
		time.Sleep(500 * time.Millisecond)
	}
	log.Printf("[PRODUCER] Done! %d tasks added to the queue.", NUM_TASKS)
}

func runWorker(workerID string) {
	delay := time.Duration(rand.Intn(3)) * time.Second
	log.Printf("[WORKER-%s] Starting in %v...", workerID, delay)
	time.Sleep(delay)
	sess, err := client.InitSession(api.ClientID("worker_" + workerID))
	if err != nil {
		log.Fatalf("[WORKER-%s] Cannot connect: %v", workerID, err)
	}
	log.Printf("[WORKER-%s] Connected! Competing for tasks...", workerID)
	processedCount, failedCount := 0, 0
	for i := 1; i <= NUM_TASKS; i++ {
		taskID := fmt.Sprintf("task_%d", i)
		lockPath := api.FilePath(TASK_LOCK_PREFIX + taskID)
		sess.OpenLock(lockPath)
		log.Printf("[WORKER-%s] Trying to acquire lock for %s...", workerID, taskID)
		ok, err := sess.TryAcquireLock(lockPath, api.EXCLUSIVE)
		if err != nil {
			log.Printf("[WORKER-%s] Error acquiring lock %s: %v", workerID, taskID, err)
			failedCount++
			continue
		}
		if !ok {
			log.Printf("[WORKER-%s] Task %s is handled by another worker, skipping.", workerID, taskID)
			failedCount++
			continue
		}
		log.Printf("[WORKER-%s] Lock acquired for %s!", workerID, taskID)
		content, err := sess.ReadContent(lockPath)
		if err != nil {
			log.Printf("[WORKER-%s] Error reading %s: %v", workerID, taskID, err)
			sess.ReleaseLock(lockPath)
			continue
		}
		var task Task
		if err := json.Unmarshal([]byte(content), &task); err != nil {
			sess.ReleaseLock(lockPath)
			continue
		}
		if task.Status != "pending" {
			log.Printf("[WORKER-%s] Task %s already processed, skipping.", workerID, taskID)
			sess.ReleaseLock(lockPath)
			continue
		}
		task.Status = "processing"
		task.AssignedTo = "worker_" + workerID
		updatedJSON, _ := json.Marshal(task)
		sess.WriteContent(lockPath, string(updatedJSON))
		log.Printf("[WORKER-%s] Processing: %s | type: %s", workerID, taskID, task.Type)
		processingTime := time.Duration(1+rand.Intn(3)) * time.Second
		time.Sleep(processingTime)
		task.Status = "done"
		task.CompletedAt = time.Now()
		doneJSON, _ := json.Marshal(task)
		sess.WriteContent(lockPath, string(doneJSON))
		log.Printf("[WORKER-%s] Completed %s | type: %-20s | duration: %v", workerID, taskID, task.Type, processingTime)
		sess.ReleaseLock(lockPath)
		log.Printf("[WORKER-%s] Lock released for %s", workerID, taskID)
		processedCount++
		time.Sleep(100 * time.Millisecond)
	}
	log.Printf("[WORKER-%s] Summary: processed %d | skipped %d", workerID, processedCount, failedCount)
}

func main() {
	var mode, workerID string
	flag.StringVar(&mode, "mode", "", "Run mode: producer or worker")
	flag.StringVar(&workerID, "id", "1", "Worker ID")
	flag.Parse()
	if mode == "" {
		fmt.Println("Usage: -mode producer | -mode worker -id 1")
		os.Exit(1)
	}
	rand.Seed(time.Now().UnixNano())
	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	switch mode {
	case "producer":
		runProducer(workerID)
	case "worker":
		go runWorker(workerID)
		<-quitCh
	default:
		log.Fatalf("Invalid mode: %s", mode)
	}
}