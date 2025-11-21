package pb

import (
	"testing"
	"unsafe"
)

func calcEntriesSim(ps, cl int) int {
	target := min(cl, 32+32*(ps>>3))
	overhead := 8 + ps
	if target <= overhead {
		return 0
	}
	e := max(min((target-overhead)/ps, 7), 0)
	return e
}

func TestEntriesPerBucket_Simulated(t *testing.T) {
	cases := []struct {
		ps   int
		cl   int
		want int
	}{
		{8, 32, 2},
		{8, 64, 6},
		{8, 128, 6},
		{8, 256, 6},
		{4, 32, 5},
		{4, 64, 5},
		{4, 128, 5},
		{4, 256, 5},
	}
	for _, c := range cases {
		got := calcEntriesSim(c.ps, c.cl)
		if got != c.want {
			t.Fatalf("ps=%d cl=%d got=%d want=%d", c.ps, c.cl, got, c.want)
		}
		target := min(c.cl, 32+32*(c.ps>>3))
		overhead := 8 + c.ps
		bucket := overhead + got*c.ps
		if !(bucket <= target && target-bucket < c.ps) {
			t.Fatalf("ps=%d cl=%d bucket=%d target=%d", c.ps, c.cl, bucket, target)
		}
		if got > 7 {
			t.Fatalf("ps=%d cl=%d got=%d", c.ps, c.cl, got)
		}
	}
}

func TestEntriesPerBucket_Actual(t *testing.T) {
	ps := int(unsafe.Sizeof(unsafe.Pointer(nil)))
	cl := int(CacheLineSize)
	exp := calcEntriesSim(ps, cl)
	if entriesPerBucket != exp {
		t.Fatalf("entriesPerBucket=%d exp=%d", entriesPerBucket, exp)
	}
	size := unsafe.Sizeof(bucketOf{})
	overhead := uintptr(8 + ps)
	want := overhead + uintptr(entriesPerBucket*ps)
	if size != want {
		t.Fatalf("bucket size=%d want=%d", size, want)
	}
}
