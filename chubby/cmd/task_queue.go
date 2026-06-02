package main 

import(
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
	ID string `json:"id"`
	Type string `json:"type"`
	Status string `json:"status"` // pending, processing, completed
	AssignedTo string `json:"assigned_to"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"completed_at,omitempty"`
}


var taskTypes = []string{
	"send_email",
	"generate_report",
	"process_payment",
	"resize_image",
	"send_notification",
	"backup_database",
	"sync_data",
	"calculate_stats",
}

const NUM_TASKS = 8

const TASK_LOCK_PREFIX = "TaskQueue/task_"
const TASK_META_PREFIX = "TaskQueue/meta_"
const QUEUE_LOCK =  "TaskQueue/queue_lock"

