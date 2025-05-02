package mongodb

import (
	"reflect"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func GetUpdatedFields(i any) bson.D {
	v := reflect.ValueOf(i).Elem()
	t := reflect.TypeOf(i).Elem()

	fields := bson.D{}
	for i := range v.NumField() {
		fieldVal := v.Field(i)
		fieldTyp := t.Field(i)
		bsonTag := fieldTyp.Tag.Get("bson")

		if fieldVal.IsNil() {
			continue
		}

		val := fieldVal.Interface()
		if fieldVal.Kind() == reflect.Ptr {
			val = fieldVal.Elem().Interface()
		}

		fields = append(fields, bson.E{Key: bsonTag, Value: val})
	}

	return fields
}
