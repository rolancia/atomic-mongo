package database

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
	"strings"
	"time"
)

const (
	timeout    = 5 * time.Second
	retryCount = 10
	retryDelay = 100 * time.Millisecond
)

type DbName string

type DocumentInterface interface {
	ignored() map[string]bool
	document() *Document
	Collection() string
	DocumentPtr() interface{}
	DataPtr() interface{}
}

type Document struct {
	ID           *primitive.ObjectID `json:"_id,omitempty" bson:"_id,omitempty"`
	excluded     map[string]bool
	withOutID    bool
	customFilter primitive.D
}

func (md *Document) WithOutID(b bool) {
	md.withOutID = b
}

func (md *Document) documentIDPtr() interface{} {
	return md.ID
}

func (md *Document) ignored() map[string]bool {
	return md.excluded
}

func (md *Document) document() *Document {
	return md
}

func (md *Document) SetCustomFilter(f primitive.D) {
	md.customFilter = f
}

func (md *Document) CustomFilter() primitive.D {
	return md.customFilter
}

func (md *Document) UndoNoFetch(fieldName string) {
	if md.excluded == nil {
		md.excluded = map[string]bool{}
	}

	md.excluded[fieldName] = false
}

func (md *Document) NoFetch(fieldName string) {
	if md.excluded == nil {
		md.excluded = map[string]bool{}
	}

	md.excluded[fieldName] = true
}

func projection(mdi DocumentInterface) bson.M {
	proj := bson.M{}

	vof := reflect.ValueOf(mdi.DataPtr())
	ind := reflect.Indirect(vof)
	for i := 0; i < ind.NumField(); i++ {
		field := ind.Type().Field(i)
		fieldName, jsonTag := field.Name, field.Tag.Get("json")

		// Document 처리
		if fieldName == "DocumentIdentity" {
			md := mdi.document()
			proj["_id"] = boolToInt(!md.withOutID)
			continue
		}

		// 그 외 처리
		if jsonTag == "" {
			continue
		}

		isExcluded := mdi.ignored()[fieldName]
		if isExcluded {
			proj[jsonTag] = 0
		} else {
			proj[jsonTag] = 1
		}
	}

	return proj
}

func Projection(mdi DocumentInterface) bson.M {
	return projection(mdi)
}

func marshalToInsert(mdi DocumentInterface) map[string]interface{} {
	ret := map[string]interface{}{}

	vof := reflect.ValueOf(mdi.DataPtr())
	ind := reflect.Indirect(vof)
	for i := 0; i < ind.NumField(); i++ {
		field := ind.Type().Field(i)
		fieldName, jsonTag := field.Name, field.Tag.Get("json")
		if fieldName == "ID" || jsonTag == "" {
			continue
		}

		if ignored := mdi.document().ignored()[fieldName]; ignored {
			continue
		}

		val := vof.Elem().Field(i).Interface()
		ret[jsonTag] = val
	}

	return ret
}

func marshalToUpdate(mdi DocumentInterface) map[string]interface{} {
	ret := map[string]interface{}{}

	vof := reflect.ValueOf(mdi.DataPtr())
	ind := reflect.Indirect(vof)
	for i := 0; i < ind.NumField(); i++ {
		field := ind.Type().Field(i)
		fieldName, jsonTag := field.Name, field.Tag.Get("json")
		if fieldName == "ID" || jsonTag == "" || strings.HasPrefix(jsonTag, "_") {
			continue
		}

		if ignored := mdi.document().ignored()[fieldName]; ignored {
			continue
		}

		val := vof.Elem().Field(i).Interface()
		ret[jsonTag] = val
	}

	return ret
}

func boolToInt(b bool) int {
	if b {
		return 1
	} else {
		return 0
	}
}

func knownError(err error) bool {
	switch err {
	case mongo.ErrClientDisconnected, mongo.ErrEmptySlice, mongo.ErrInvalidIndexValue, mongo.ErrMissingResumeToken, mongo.ErrMultipleIndexDrop, mongo.ErrNilCursor, mongo.ErrNilDocument, mongo.ErrNilValue, mongo.ErrNoDocuments, mongo.ErrNonStringIndexName, mongo.ErrWrongClient:
		return true
	case mongo.ErrUnacknowledgedWrite:
		return false
	default:
		return false
	}
}

func insert(ctx context.Context, mdi DocumentInterface, dbName DbName) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	col := GetMongoDB(ctx, dbName).Collection(mdi.Collection())
	result, err := col.InsertOne(ctx, marshalToInsert(mdi))
	if err != nil {
		return err
	}

	inserted := result.InsertedID.(primitive.ObjectID)
	mdi.document().ID = &inserted

	return nil
}

func Insert(ctx context.Context, mdi DocumentInterface, name DbName) error {
	var err error
	for i := 0; i < retryCount; i++ {
		err = insert(ctx, mdi, name)
		if err == nil {
			return nil
		}

		if knownError(err) {
			return err
		} else {
			time.Sleep(retryDelay)
		}
	}
	return err
}

func Delete(ctx context.Context, mdi DocumentInterface, dbName DbName) error {
	col := GetMongoDB(ctx, dbName).Collection(mdi.Collection())
	f := mdi.document().customFilter
	if f == nil {
		f = primitive.D{{"_id", *mdi.document().ID}}
	}

	for i := 0; i < retryCount; i++ {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		_, err := col.DeleteOne(ctx, f)
		if err != nil {
			if knownError(err) {
				cancel()
				return err
			}
		}

		cancel()
		time.Sleep(retryDelay)
	}

	return nil
}

func Fetch(ctx context.Context, mdi DocumentInterface, dbName DbName) error {
	col := GetMongoDB(ctx, dbName).Collection(mdi.Collection())
	f := mdi.document().customFilter
	if f == nil {
		f = primitive.D{{"_id", *mdi.document().ID}}
	}

	var err error
	for i := 0; i < retryCount; i++ {
		ctx, cancel := context.WithTimeout(ctx, timeout)

		raw, err := col.FindOne(ctx, f, &options.FindOneOptions{
			Projection: projection(mdi),
		}).DecodeBytes()
		if err == nil {
			cancel()
			// objectID와 데이터가 다른 스트럭트에 존재하므로 따로 디코드해줌
			lookup := raw.Lookup("_id")
			objID := lookup.ObjectID()
			if objID.IsZero() == false {
				mdi.document().ID = &objID
			}

			cancel()
			return bson.Unmarshal(raw, mdi.DataPtr())
		}

		if knownError(err) {
			cancel()
			return err
		} else {
			cancel()
			time.Sleep(retryDelay)
		}
	}

	return err
}

func AtomicUpdate(ctx context.Context, mdi DocumentInterface, dbName DbName, updateFn func(ctx context.Context) error) error {
	var err error
	for i := 0; i < retryCount; i++ {
		ctx, cancel := context.WithTimeout(ctx, timeout)

		var sess mongo.Session
		sess, err = GetMongoClient(ctx).StartSession()
		if err != nil {
			if knownError(err) {
				cancel()
				return err
			} else {
				cancel()
				time.Sleep(retryDelay)
				continue
			}
		}

		retry := true
		err = mongo.WithSession(ctx, sess, func(ctx mongo.SessionContext) error {
			if err := sess.StartTransaction(); err != nil {
				cancel()
				return err
			}
			defer sess.EndSession(ctx)

			mCtx := WithMongoClient(ctx, ctx.Client())

			col := GetMongoDB(mCtx, dbName).Collection(mdi.Collection())

			if err := Fetch(mCtx, mdi, dbName); err != nil {
				if knownError(err) {
					retry = false
				}
				return err
			}

			shouldRetry, err := internalUpdate(mCtx, col, mdi, updateFn)
			if err != nil {
				retry = shouldRetry
				return err
			}

			if err := sess.CommitTransaction(ctx); err != nil {
				return err
			}

			return nil
		})
		cancel()
		if err != nil {
			if retry {
				continue
			} else {
				return err
			}
		}

		break
	}

	return nil
}

func internalUpdate(mCtx context.Context, col *mongo.Collection, mdi DocumentInterface, updateFn func(ctx context.Context) error) (retry bool, _ error) {
	var err error
	mdi.document().withOutID = true
	// 사용자 업데이트 함수에서 생긴 에러는 그냥 바이
	if err := updateFn(mCtx); err != nil {
		return false, err
	}

	var result *mongo.UpdateResult
	filter := bson.M{"_id": mdi.document().ID}
	set := bson.D{
		{"$set", marshalToUpdate(mdi)},
	}

	result, err = col.UpdateOne(mCtx, filter, set)
	if err != nil {
		retry := !knownError(err)
		return retry, err
	}

	if result.MatchedCount == 0 {
		return false, mongo.ErrNilDocument
	}

	if result.ModifiedCount == 0 {
		return false, nil
	}

	return false, nil
}

func AtomicMultipleUpdate(ctx context.Context, mdis []DocumentInterface, dbName DbName, updateFn func(ctx context.Context) error) error {
	var err error
	for i := 0; i < retryCount; i++ {
		ctx, cancel := context.WithTimeout(ctx, timeout)

		var sess mongo.Session
		sess, err = GetMongoClient(ctx).StartSession()
		if err != nil {
			if knownError(err) {
				cancel()
				return err
			} else {
				cancel()
				time.Sleep(retryDelay)
				continue
			}
		}

		retry := true
		err = mongo.WithSession(ctx, sess, func(ctx mongo.SessionContext) error {
			if err := sess.StartTransaction(); err != nil {
				cancel()
				return err
			}
			defer sess.EndSession(ctx)

			mCtx := WithMongoClient(ctx, ctx.Client())

			cols := []*mongo.Collection{}
			for i := range mdis {
				col := GetMongoDB(mCtx, dbName).Collection(mdis[i].Collection())
				cols = append(cols, col)
				if err := Fetch(mCtx, mdis[i], dbName); err != nil {
					if knownError(err) {
						retry = false
					}
					return err
				}
			}

			for i := range mdis {
				shouldRetry, err := internalUpdate(mCtx, cols[i], mdis[i], updateFn)
				if err != nil {
					retry = shouldRetry
					return err
				}
			}

			return nil
		})
		if err != nil {
			if retry {
				cancel()
				continue
			} else {
				cancel()
				return err
			}
		}

		cancel()
		break
	}

	return nil
}
