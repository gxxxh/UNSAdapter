package gin

import "testing"

func TestGin(t *testing.T) {
	r :=initGin()
	r.Run(":8080")
}

