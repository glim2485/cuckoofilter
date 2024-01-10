package cuckoo

import (
	"fmt"
	"math/bits"
	"math/rand"
)

const maxCuckooCount = 500

// Filter is a probabilistic counter
type Filter struct {
	Buckets   []bucket
	Count     uint
	BucketPow uint
}

// NewFilter returns a new cuckoofilter with a given capacity.
// A capacity of 1000000 is a normal default, which allocates
// about ~1MB on 64-bit machines.
func NewFilter(capacity uint) *Filter {
	capacity = getNextPow2(uint64(capacity)) / bucketSize
	if capacity == 0 {
		capacity = 1
	}
	buckets := make([]bucket, capacity)
	return &Filter{
		Buckets:   buckets,
		Count:     0,
		BucketPow: uint(bits.TrailingZeros(capacity)),
	}
}

func CopyFilter(buckets []bucket, count uint, bucketPow uint) *Filter {
	newBucket := make([]bucket, len(buckets))
	copy(newBucket, buckets)
	return &Filter{
		Buckets : newBucket,
		Count: count,
		BucketPow: bucketPow,
	}
}

// Lookup returns true if data is in the counter
func (cf *Filter) Lookup(data []byte) bool {
	i1, fp := getIndexAndFingerprint(data, cf.BucketPow)
	if cf.Buckets[i1].getFingerprintIndex(fp) > -1 {
		return true
	}
	i2 := getAltIndex(fp, i1, cf.BucketPow)
	return cf.Buckets[i2].getFingerprintIndex(fp) > -1
}

// Reset ...
func (cf *Filter) Reset() {
	for i := range cf.Buckets {
		cf.Buckets[i].reset()
	}
	cf.Count = 0
}

func randi(i1, i2 uint) uint {
	if rand.Intn(2) == 0 {
		return i1
	}
	return i2
}

// Insert inserts data into the counter and returns true upon success
func (cf *Filter) Insert(data []byte) bool {
	i1, fp := getIndexAndFingerprint(data, cf.BucketPow)
	if cf.insert(fp, i1) {
		return true
	}
	i2 := getAltIndex(fp, i1, cf.BucketPow)
	if cf.insert(fp, i2) {
		return true
	}
	return cf.reinsert(fp, randi(i1, i2))
}

// InsertUnique inserts data into the counter if not exists and returns true upon success
func (cf *Filter) InsertUnique(data []byte) bool {
	if cf.Lookup(data) {
		return false
	}
	return cf.Insert(data)
}

func (cf *Filter) insert(fp fingerprint, i uint) bool {
	if cf.Buckets[i].insert(fp) {
		cf.Count++
		return true
	}
	return false
}

func (cf *Filter) reinsert(fp fingerprint, i uint) bool {
	for k := 0; k < maxCuckooCount; k++ {
		j := rand.Intn(bucketSize)
		oldfp := fp
		fp = cf.Buckets[i][j]
		cf.Buckets[i][j] = oldfp

		// look in the alternate location for that random element
		i = getAltIndex(fp, i, cf.BucketPow)
		if cf.insert(fp, i) {
			return true
		}
	}
	return false
}

// Delete data from counter if exists and return if deleted or not
func (cf *Filter) Delete(data []byte) bool {
	i1, fp := getIndexAndFingerprint(data, cf.BucketPow)
	if cf.delete(fp, i1) {
		return true
	}
	i2 := getAltIndex(fp, i1, cf.BucketPow)
	return cf.delete(fp, i2)
}

func (cf *Filter) delete(fp fingerprint, i uint) bool {
	if cf.Buckets[i].delete(fp) {
		if cf.Count > 0 {
			cf.Count--
		}
		return true
	}
	return false
}

// Count returns the number of items in the counter
func (cf *Filter) CountEntries() uint {
	return cf.Count
}

// Encode returns a byte slice representing a Cuckoofilter
func (cf *Filter) Encode() []byte {
	bytes := make([]byte, len(cf.Buckets)*bucketSize)
	for i, b := range cf.Buckets {
		for j, f := range b {
			index := (i * len(b)) + j
			bytes[index] = byte(f)
		}
	}
	return bytes
}

// Decode returns a Cuckoofilter from a byte slice
func Decode(bytes []byte) (*Filter, error) {
	var count uint
	if len(bytes)%bucketSize != 0 {
		return nil, fmt.Errorf("expected bytes to be multiple of %d, got %d", bucketSize, len(bytes))
	}
	if len(bytes) == 0 {
		return nil, fmt.Errorf("bytes can not be empty")
	}
	buckets := make([]bucket, len(bytes)/4)
	for i, b := range buckets {
		for j := range b {
			index := (i * len(b)) + j
			if bytes[index] != 0 {
				buckets[i][j] = fingerprint(bytes[index])
				count++
			}
		}
	}
	return &Filter{
		Buckets:   buckets,
		Count:     count,
		BucketPow: uint(bits.TrailingZeros(uint(len(buckets)))),
	}, nil
}
