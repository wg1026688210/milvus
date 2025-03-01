package rootcoord

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"go.uber.org/atomic"

	"github.com/stretchr/testify/assert"
)

type mockFailTask struct {
	baseTask
	prepareErr error
	executeErr error
}

func newMockFailTask() *mockFailTask {
	task := &mockFailTask{
		baseTask: baseTask{
			ctx:  context.Background(),
			done: make(chan error, 1),
		},
	}
	task.SetCtx(context.Background())
	return task
}

func newMockPrepareFailTask() *mockFailTask {
	task := newMockFailTask()
	task.prepareErr = errors.New("error mock Prepare")
	return task
}

func newMockExecuteFailTask() *mockFailTask {
	task := newMockFailTask()
	task.prepareErr = errors.New("error mock Execute")
	return task
}

func (m mockFailTask) Prepare(context.Context) error {
	return m.prepareErr
}

func (m mockFailTask) Execute(context.Context) error {
	return m.executeErr
}

type mockNormalTask struct {
	baseTask
}

func newMockNormalTask() *mockNormalTask {
	task := &mockNormalTask{
		baseTask: baseTask{
			ctx:  context.Background(),
			done: make(chan error, 1),
		},
	}
	task.SetCtx(context.Background())
	return task
}

func Test_scheduler_Start_Stop(t *testing.T) {
	idAlloc := newMockIDAllocator()
	tsoAlloc := newMockTsoAllocator()
	ctx := context.Background()
	s := newScheduler(ctx, idAlloc, tsoAlloc)
	s.Start()
	s.Stop()
}

func Test_scheduler_failed_to_set_id(t *testing.T) {
	idAlloc := newMockIDAllocator()
	tsoAlloc := newMockTsoAllocator()
	idAlloc.AllocOneF = func() (UniqueID, error) {
		return 0, errors.New("error mock AllocOne")
	}
	ctx := context.Background()
	s := newScheduler(ctx, idAlloc, tsoAlloc)
	s.Start()
	defer s.Stop()
	task := newMockNormalTask()
	err := s.AddTask(task)
	assert.Error(t, err)
}

func Test_scheduler_failed_to_set_ts(t *testing.T) {
	idAlloc := newMockIDAllocator()
	tsoAlloc := newMockTsoAllocator()
	idAlloc.AllocOneF = func() (UniqueID, error) {
		return 100, nil
	}
	tsoAlloc.GenerateTSOF = func(count uint32) (uint64, error) {
		return 0, errors.New("error mock GenerateTSO")
	}
	ctx := context.Background()
	s := newScheduler(ctx, idAlloc, tsoAlloc)
	s.Start()
	defer s.Stop()
	task := newMockNormalTask()
	err := s.AddTask(task)
	assert.Error(t, err)
}

func Test_scheduler_enqueue_normal_case(t *testing.T) {
	idAlloc := newMockIDAllocator()
	tsoAlloc := newMockTsoAllocator()
	idAlloc.AllocOneF = func() (UniqueID, error) {
		return 100, nil
	}
	tsoAlloc.GenerateTSOF = func(count uint32) (uint64, error) {
		return 101, nil
	}
	ctx := context.Background()
	s := newScheduler(ctx, idAlloc, tsoAlloc)
	s.Start()
	defer s.Stop()
	task := newMockNormalTask()
	err := s.AddTask(task)
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(100), task.GetID())
	assert.Equal(t, Timestamp(101), task.GetTs())
}

func Test_scheduler_bg(t *testing.T) {
	idAlloc := newMockIDAllocator()
	tsoAlloc := newMockTsoAllocator()
	idAlloc.AllocOneF = func() (UniqueID, error) {
		return 100, nil
	}
	tsoAlloc.GenerateTSOF = func(count uint32) (uint64, error) {
		return 101, nil
	}
	ctx := context.Background()
	s := newScheduler(ctx, idAlloc, tsoAlloc)
	s.Start()

	n := 10
	tasks := make([]task, 0, n)
	for i := 0; i < n; i++ {
		which := rand.Int() % 3
		switch which {
		case 0:
			tasks = append(tasks, newMockPrepareFailTask())
		case 1:
			tasks = append(tasks, newMockExecuteFailTask())
		default:
			tasks = append(tasks, newMockNormalTask())
		}
	}

	for _, task := range tasks {
		s.AddTask(task)
	}

	for _, task := range tasks {
		err := task.WaitToFinish()
		switch task.(type) {
		case *mockFailTask:
			assert.Error(t, err)
		case *mockNormalTask:
			assert.NoError(t, err)
		}
	}

	s.Stop()
}

func Test_scheduler_updateDdlMinTsLoop(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		idAlloc := newMockIDAllocator()
		tsoAlloc := newMockTsoAllocator()
		tso := atomic.NewUint64(100)
		idAlloc.AllocOneF = func() (UniqueID, error) {
			return 100, nil
		}
		tsoAlloc.GenerateTSOF = func(count uint32) (uint64, error) {
			got := tso.Inc()
			return got, nil
		}
		ctx := context.Background()
		s := newScheduler(ctx, idAlloc, tsoAlloc)
		Params.InitOnce()
		Params.ProxyCfg.TimeTickInterval = time.Millisecond
		s.Start()

		time.Sleep(time.Millisecond * 4)

		assert.Greater(t, s.GetMinDdlTs(), Timestamp(100))

		// add task to queue.
		n := 10
		for i := 0; i < n; i++ {
			task := newMockNormalTask()
			err := s.AddTask(task)
			assert.NoError(t, err)
		}

		time.Sleep(time.Millisecond * 4)
		s.Stop()
	})

	t.Run("invalid tso", func(t *testing.T) {
		idAlloc := newMockIDAllocator()
		tsoAlloc := newMockTsoAllocator()
		idAlloc.AllocOneF = func() (UniqueID, error) {
			return 100, nil
		}
		tsoAlloc.GenerateTSOF = func(count uint32) (uint64, error) {
			return 0, fmt.Errorf("error mock GenerateTSO")
		}
		ctx := context.Background()
		s := newScheduler(ctx, idAlloc, tsoAlloc)
		Params.InitOnce()
		Params.ProxyCfg.TimeTickInterval = time.Millisecond
		s.Start()

		time.Sleep(time.Millisecond * 4)
		assert.Zero(t, s.GetMinDdlTs())
		s.Stop()
	})
}
