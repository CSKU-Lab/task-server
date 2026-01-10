package mongodb

import (
	"reflect"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func GetUpdatedFields(i any) bson.M {
	v := reflect.ValueOf(i).Elem()
	t := reflect.TypeOf(i).Elem()

	fields := make(bson.M)
	for i := range v.NumField() {
		fieldVal := v.Field(i)
		fieldTyp := t.Field(i)
		bsonTag := fieldTyp.Tag.Get("bson")

		if fieldVal.IsNil() {
			continue
		}

		val := fieldVal.Interface()
		if fieldVal.Kind() == reflect.Pointer {
			val = fieldVal.Elem().Interface()
		}

		fields[bsonTag] = val
	}

	return fields
}
