package sriracha_test

import (
	"fmt"

	"go.sriracha.dev/sriracha"
)

func ExampleParseFieldPath() {
	fp, err := sriracha.ParseFieldPath("sriracha::name::given")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(fp.Org(), fp.Namespace(), fp.LocalName())
	// Output: sriracha name given
}
