package topology

import "strings"

func isValidNamespacedKind(value string) bool {
	segments := strings.Split(value, ".")
	if len(segments) < 2 {
		return false
	}

	for _, segment := range segments {
		if !isValidKindSegment(segment) {
			return false
		}
	}

	return true
}

func isValidKindSegment(segment string) bool {
	for index, character := range segment {
		if character >= 'a' && character <= 'z' {
			continue
		}
		if index > 0 && (character >= '0' && character <= '9' || character == '_') {
			continue
		}
		return false
	}

	return segment != ""
}
