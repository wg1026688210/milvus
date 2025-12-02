package roles

import (
	"fmt"
	"github.com/go-co-op/gocron"
	"golang.org/x/sys/unix"
	"testing"
	"time"
)

func TestScheduler(t *testing.T) {
	timezone, _ := time.LoadLocation("Asia/Shanghai")
	scheduler := gocron.NewScheduler(timezone)
	fmt.Printf("aaaaaapid, %v,ppid，%v", unix.Getpid(), unix.Getppid())

	scheduler.Every(1).Seconds().Do(
		func() {
			fmt.Printf("pid, %v,ppid，%v", unix.Getpid(), unix.Getppid())
		})
	scheduler.StartBlocking()
}
