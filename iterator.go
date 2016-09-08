package redis

import "sync"

type Scanner struct {
	client *cmdable
	*ScanCmd
}

// Iterator creates a new ScanIterator.
func (s Scanner) Iterator() *ScanIterator {
	return &ScanIterator{
		Scanner: s,
	}
}

// ScanIterator is used to incrementally iterate over a collection of elements.
// It's safe for concurrent use by multiple goroutines.
type ScanIterator struct {
	mu sync.Mutex // protects Scanner and pos
	Scanner
	pos int
}

// Err returns the last iterator error, if any.
func (it *ScanIterator) Err() error {
	it.mu.Lock()
	err := it.ScanCmd.Err()
	it.mu.Unlock()
	return err
}

// Next advances the cursor and returns true if more values can be read.
func (it *ScanIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	// Instantly return on errors.
	if it.ScanCmd.Err() != nil {
		return false
	}

	// Advance cursor, check if we are still within range.
	if it.pos < len(it.ScanCmd.page) {
		it.pos++
		return true
	}

	for {
		// Return if there is no more data to fetch.
		if it.ScanCmd.cursor == 0 {
			return false
		}

		// Fetch next page.
		if it.ScanCmd._args[0] == "scan" {
			it.ScanCmd._args[1] = it.ScanCmd.cursor
		} else {
			it.ScanCmd._args[2] = it.ScanCmd.cursor
		}
		it.ScanCmd.reset()
		it.client.process(it.ScanCmd)
		if it.ScanCmd.Err() != nil {
			return false
		}

		it.pos = 1

		// Redis can occasionally return empty page
		if len(it.ScanCmd.page) > 0 {
			return true
		}
	}
	return false
}

// Val returns the key/field at the current cursor position.
func (it *ScanIterator) Val() string {
	var v string
	it.mu.Lock()
	if it.ScanCmd.Err() == nil && it.pos > 0 && it.pos <= len(it.ScanCmd.page) {
		v = it.ScanCmd.page[it.pos-1]
	}
	it.mu.Unlock()
	return v
}
