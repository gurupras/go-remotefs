package remotefs

func convertOpenRequestFlagsToInt(flags []string) int {
	result := 0
	for _, flag := range flags {
		val := openFlagsMap[flag]
		result |= val
	}
	return result
}
