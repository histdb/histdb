package level0

type keyHeap []keyPos

func (kh keyHeap) Push(key keyPos) keyHeap {
	kh = append(kh, key)
	kh.heapUp()
	return kh
}

func (kh keyHeap) Pop() (_ keyHeap, key keyPos) {
	last := len(kh) - 1

	if len(kh) > 0 {
		key, kh[0] = kh[0], kh[last]
		kh = kh[:last]
		kh.heapDown()
	}

	return kh, key
}

func (kh keyHeap) heapUp() {
	i := uint(len(kh) - 1)

next:
	if j := (i - 1) / 2; i != j && i < uint(len(kh)) && j < uint(len(kh)) {
		ip, jp := &kh[i], &kh[j]
		if string(ip.key[:]) < string(jp.key[:]) {
			*ip, *jp, i = *jp, *ip, j
			goto next
		}
	}
}

func (kh keyHeap) heapDown() {
	i := uint(0)

next:
	if j := 2*i + 1; i < uint(len(kh)) && j < uint(len(kh)) {
		ip, jp := &kh[i], &kh[j]

		if jn := j + 1; jn < uint(len(kh)) &&
			string(kh[jn].key[:]) < string(jp.key[:]) {
			jp, j = &kh[jn], jn
		}

		if string(jp.key[:]) < string(ip.key[:]) {
			*ip, *jp, i = *jp, *ip, j
			goto next
		}
	}
}
