package util

func MustPanic(err error) {
	if err != nil {
		panic(err)
	}
}
