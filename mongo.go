package database

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type mongoClientCtxKey string

func WithMongo(ctx context.Context, mongoURI string) (context.Context, error) {
	tCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(tCtx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return ctx, err
	}

	mCtx := context.WithValue(ctx, mongoClientCtxKey(""), client)
	return mCtx, nil
}

func WithMongoClient(ctx context.Context, client *mongo.Client) context.Context {
	cCtx := context.WithValue(ctx, mongoClientCtxKey(""), client)
	return cCtx
}

func GetMongoClient(ctx context.Context) *mongo.Client {
	return ctx.Value(mongoClientCtxKey("")).(*mongo.Client)
}

func GetMongoDB(ctx context.Context, name DbName, opts ...*options.DatabaseOptions) *mongo.Database {
	return GetMongoClient(ctx).Database(string(name), opts...)
}
