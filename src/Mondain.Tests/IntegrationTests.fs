namespace Mondain.IntegrationTests
open Mondain
open Xunit
open Mondain.Builders
open Mondain.TestModels
open MongoDB.Driver

type MyTests(connection: MongoConnection) =
    [<Fact>]
    member _.``Basic Test``() = 
        task {
            let collection = connection.GetCollection<Demo>()
            let demo = { Id = ""; Name = "name"; Version = 1; Members = [] }
            do! collection.InsertOneAsync(demo)
            let! q =
                findUpdate {
                    for c in collection do   
                        where (c.Id = demo.Id)
                        update(c.Version += 3)
                        returnDoc ReturnDocument.Before
                        select {| a = c.Name |}
                        //limit (10)
                }

            //let enumerator = q.GetAsyncEnumerator()
            //let! m = enumerator.MoveNextAsync()
            //let mutable hasMore = m
            //while hasMore do
            //    let value = enumerator.Current
            //    let! m = enumerator.MoveNextAsync()
            //    let mutable hasMore = m
            //    ()
            ()
        }
        
    interface IClassFixture<MongoConnection>

