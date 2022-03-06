namespace Mondain

open Mongo2Go
open MongoDB.Driver
open MongoDB.FSharp

open System
open MongoDB.Bson.Serialization.Conventions

type MongoConnection() = 
    let pack = new ConventionPack()
    
    do
        pack.Add( StringIdStoredAsObjectIdConvention() )
        ConventionRegistry.Register("Custom Convention", pack, fun t -> true)
        //Serializers.Register()

    let runner = MongoDbRunner.Start()

    member _.GetCollection<'T>() =
        MongoClient(runner.ConnectionString)
            .GetDatabase("IntegrationTest")
            .GetCollection<'T>(typeof<'T>.Name)
    
    interface IDisposable with
        member _.Dispose() = runner.Dispose()