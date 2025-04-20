package redis

import (
	"context"
	"iter"
)

// ScanIterator is used to incrementally iterate over a collection of elements.
type ScanIterator struct {
	cmd *ScanCmd
	pos int
}

// Err returns the last iterator error, if any.
func (it *ScanIterator) Err() error {
	return it.cmd.Err()
}

// Next advances the cursor and returns true if more values can be read.
//
// Deprecated: support for native iterators has been added in go 1.23. Use Vals instead.
func (it *ScanIterator) Next(ctx context.Context) bool {
	// Instantly return on errors.
	if it.cmd.Err() != nil {
		return false
	}

	// Advance cursor, check if we are still within range.
	if it.pos < len(it.cmd.page) {
		it.pos++
		return true
	}

	for {
		// Return if there is no more data to fetch.
		if it.cmd.cursor == 0 {
			return false
		}

		// Fetch next page.
		switch it.cmd.args[0] {
		case "scan", "qscan":
			it.cmd.args[1] = it.cmd.cursor
		default:
			it.cmd.args[2] = it.cmd.cursor
		}

		err := it.cmd.process(ctx, it.cmd)
		if err != nil {
			return false
		}

		it.pos = 1

		// Redis can occasionally return empty page.
		if len(it.cmd.page) > 0 {
			return true
		}
	}
}

// Val returns the key/field at the current cursor position.
//
// Deprecated: support for native iterators has been added in go 1.23. Use Vals instead.
func (it *ScanIterator) Val() string {
	var v string
	if it.cmd.Err() == nil && it.pos > 0 && it.pos <= len(it.cmd.page) {
		v = it.cmd.page[it.pos-1]
	}
	return v
}

// Vals returns iterator over key/field at the current cursor position.
func (it *ScanIterator) Vals(ctx context.Context) iter.Seq[string] {
	return func(yield func(string) bool) {
		if it.cmd.Err() != nil {
			return
		}

		for {
			for _, val := range it.cmd.page {
				if !yield(val) {
					return
				}
			}

			// Return if there is no more data to fetch.
			if it.cmd.cursor == 0 {
				return
			}

			// Fetch next page.
			var cursorIndex int
			switch it.cmd.args[0] {
			case "scan", "qscan":
				cursorIndex = 1
			default:
				cursorIndex = 2
			}

			it.cmd.args[cursorIndex] = it.cmd.cursor

			err := it.cmd.process(ctx, it.cmd)
			if err != nil {
				return
			}
		}
	}
}
