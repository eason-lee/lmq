package ut

import "github.com/samber/lo"

func Map[T any, R any](collection []T, fn func(T) R) []R {
	return lo.Map(collection, func(t T, _ int) R { return fn(t) })
}
