package stringy

// Add 将 a 加入 s
func Add(s []string, a string) []string {
	for _, existing := range s {
		if a == existing {
			return s
		}
	}
	return append(s, a)

}

// Union 合并 s 和 a 两个 slice
func Union(s []string, a []string) []string {
	for _, entry := range a {
		found := false
		for _, existing := range s {
			if entry == existing {
				found = true
				break
			}
		}
		if !found {
			s = append(s, entry)
		}
	}
	return s
}

// Uniq 将 s slice 去重
func Uniq(s []string) (r []string) {
	for _, entry := range s {
		found := false
		for _, existing := range r {
			if existing == entry {
				found = true
				break
			}
		}
		if !found {
			r = append(r, entry)
		}
	}
	return
}
