package redis

import (
	"context"
	"errors"
	"testing"
)

// fdBudgetProbe is a stub engine whose Close flush records the retry bound of
// each pipeline it runs.
func fdBudgetProbe() (*fdEngine, *[]int) {
	var bounds []int
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{MaxRetries: 3}}},
		maxBatch: 8,
		runPipeline: func(_ context.Context, _ []Cmder, maxRetries int) error {
			bounds = append(bounds, maxRetries)
			return nil
		},
	}
	return fd, &bounds
}

// One FD pipeline flushed on Close can carry commands with different attempt
// counts. It runs as one pipeline, so it gets the smallest remaining budget
// of its commands: the budget of its first command let a later command with
// more attempts run past its MaxRetries+1 executions.
func TestFDCloseFlushUsesSmallestBudgetOfAPipeline(t *testing.T) {
	ctx := context.Background()
	fd, bounds := fdBudgetProbe()
	carry := fdPipeGroupReqs(ctx, 1, 3) // budgets 3 and 1 with MaxRetries 3
	if err := fd.flushCarryBudgeted(ctx, carry); err != nil {
		t.Fatalf("flushCarryBudgeted: %v", err)
	}
	if got := *bounds; len(got) != 1 || got[0] != 1 {
		t.Fatalf("pipeline retry bounds %v, want [1] (the smallest budget)", got)
	}
}

// When any command of the pipeline has spent its budget, the pipeline does not
// run: running it would execute that command again.
func TestFDCloseFlushFailsPipelineWithSpentBudget(t *testing.T) {
	ctx := context.Background()
	fd, bounds := fdBudgetProbe()
	carry := fdPipeGroupReqs(ctx, 1, 5) // the second has run past MaxRetries+1
	if err := fd.flushCarryBudgeted(ctx, carry); err != nil {
		t.Fatalf("flushCarryBudgeted: %v", err)
	}
	if got := *bounds; len(got) != 0 {
		t.Fatalf("the pipeline ran with bounds %v; a command in it had spent its budget", got)
	}
	for i, r := range carry {
		if !errors.Is(r.cmd.Err(), errFDRetryBudgetExhausted) {
			t.Fatalf("command %d err=%v, want errFDRetryBudgetExhausted", i, r.cmd.Err())
		}
	}
}
