// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package bufio2

type sliceAlloc struct {
	buf []byte
}

func (d *sliceAlloc) Make(n int) (ss []byte) {
	switch {
	case n == 0:
		return []byte{}
	case n >= 512:
		return make([]byte, n)
	default:
		if len(d.buf) < n {
			d.buf = make([]byte, 8192)
		}
		ss, d.buf = d.buf[:n:n], d.buf[n:]
		return ss
	}
}
