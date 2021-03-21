package example

import (
	am "atomic-mongo"
	"context"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
)

func Do() {
	ctx := context.Background()
	ctx, _ = am.WithMongo(ctx, "...")

	dbName := am.DbName("db")

	//// insert
	personDoc := PersonDocument{
		Person: &Person{
			Name: "Tae",
			Age:  28,
		},
	}

	_ = am.Insert(ctx, &personDoc, dbName)

	//// fetch
	// after insert, ID fetched.
	personInsertID := personDoc.ID

	fetchingPerson := PersonDocument{
		Document: am.Document{ID: personInsertID},
		Person: &Person{
			Name: "Tae",
		},
	}

	_ = am.Fetch(ctx, &fetchingPerson, dbName)

	// custom filter (_id default)
	fetchingPerson.SetCustomFilter(bson.D{{"name", "Tae"}})

	_ = am.Fetch(ctx, &fetchingPerson, dbName)

	// partial fetch
	fetchingPerson.NoFetch("Age")
	_ = am.Fetch(ctx, &fetchingPerson, dbName)

	fetchingPerson.UndoNoFetch("Age")
	_ = am.Fetch(ctx, &fetchingPerson, dbName)

	//// update
	_ = am.AtomicUpdate(ctx, &fetchingPerson, dbName, func(ctx context.Context) error {
		if fetchingPerson.Age > 80 {
			fetchingPerson.Name = "-"
		}

		if fetchingPerson.Name == "GOD" {
			// if error returned in this function, will not try to update.
			return errors.New("never die")
		}

		return nil
	})

	//// multiple atomic update (tx)
	anotherPerson := PersonDocument{
		Person: &Person{
			Name: "Chicken",
			Age:  1,
		},
	}

	_ = am.Insert(ctx, &anotherPerson, dbName)

	persons := []am.DocumentInterface{&fetchingPerson, &anotherPerson}
	_ = am.AtomicMultipleUpdate(ctx, persons, dbName, func(ctx context.Context) error {
		if fetchingPerson.Name == anotherPerson.Name {
			// if error returned in this function, will not try to update.
			return errors.New("same")
		}

		fetchingPerson.Name, anotherPerson.Name = anotherPerson.Name, fetchingPerson.Name

		return nil
	})
}
