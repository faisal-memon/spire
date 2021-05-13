package cache

import (
	"sync"

	"github.com/spiffe/spire/proto/spire/common"
	commonselector"github.com/spiffe/spire/pkg/common/selector"
)

var (
	stringSetPool = sync.Pool{
		New: func() interface{} {
			return make(stringSet)
		},
	}

	subscriberSetPool = sync.Pool{
		New: func() interface{} {
			return make(subscriberSet)
		},
	}

	selectorSetPool = sync.Pool{
		New: func() interface{} {
			return commonselector.NewSet()
		},
	}

	recordSetPool = sync.Pool{
		New: func() interface{} {
			return make(recordSet)
		},
	}
)

// unique set of strings, allocated from a pool
type stringSet map[string]struct{}

func allocStringSet(ss ...string) (stringSet, func()) {
	set := stringSetPool.Get().(stringSet)
	set.Merge(ss...)
	return set, func() {
		clearStringSet(set)
		stringSetPool.Put(set)
	}
}

func clearStringSet(set stringSet) {
	for k := range set {
		delete(set, k)
	}
}

func (set stringSet) Merge(ss ...string) {
	for _, s := range ss {
		set[s] = struct{}{}
	}
}

// unique set of subscribers, allocated from a pool
type subscriberSet map[*subscriber]struct{}

func allocSubscriberSet() (subscriberSet, func()) {
	set := subscriberSetPool.Get().(subscriberSet)
	return set, func() {
		clearSubscriberSet(set)
		subscriberSetPool.Put(set)
	}
}

func clearSubscriberSet(set subscriberSet) {
	for k := range set {
		delete(set, k)
	}
}

// unique set of selectors, allocated from a pool
/*type selector struct {
	Type  string
	Value string
}*/
//type selector commonselector.Selector

func makeSelector(s *common.Selector) commonselector.Selector {
	return commonselector.Selector{
		Type:  s.Type,
		Value: s.Value,
	}
}

//type selectorSet map[selector]struct{}
type selectorSet commonselector.Set

func allocSelectorSet(ss []*common.Selector) (selectorSet, func()) {
	set := selectorSetPool.Get().(selectorSet)
	MergeSelectors(set, ss)
	return set, func() {
		clearSelectorSet(set)
		selectorSetPool.Put(set)
	}
}

func clearSelectorSet(set selectorSet) {
	for _, s := range set.Array() {
		set.Remove(s)
	}
}

func MergeSelectors(set selectorSet, ss []*common.Selector) {
	for _, s := range ss {
		set.Add(commonselector.New(s))
	}
}

func MergeSelectorSet(set, other selectorSet) {
	for _, s := range other.Array() {
		set.Add(s)
	}
}

/*func (set selectorSet) In(ss ...*common.Selector) bool {
	for _, s := range ss {
		if _, ok := set[makeSelector(s)]; !ok {
			return false
		}
	}
	return true
}*/

// unique set of cache records, allocated from a pool
type recordSet map[*cacheRecord]struct{}

func allocRecordSet() (recordSet, func()) {
	set := recordSetPool.Get().(recordSet)
	return set, func() {
		clearRecordSet(set)
		recordSetPool.Put(set)
	}
}

func clearRecordSet(set recordSet) {
	for k := range set {
		delete(set, k)
	}
}
